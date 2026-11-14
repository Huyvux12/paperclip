import { createHash } from "node:crypto";
import { execFileSync, spawn } from "node:child_process";
import { mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { closeSync, openSync } from "node:fs";
import { createServer } from "node:http";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import { expect, it, vi } from "vitest";
import type { DurablePrpControlPlane } from "../control-plane/durable-prp-control-plane.js";
import { QUALIFIED_ACPX_PROFILES, resolveQualifiedAcpxProfile } from "../drivers/acpx/qualified-profiles.js";
import { createCapabilityRunnerdCodexTransport, readRunnerdArtifactBinding, type CapabilityRunnerdCodexTransport, type CapabilityRunnerdCodexTransportOptions } from "./runnerd-codex-transport.js";
import { createRunnerProviderLaunch, runnerLaunchRequirementsDigest, type RunnerProviderLaunchRequirements } from "./runner-provider-launch.js";
import { defaultCapabilityRunnerdBinary } from "../../test/support/runnerd-transport.js";
import type { PrpEvent } from "../protocol/replay-contract.js";
import { createNativeSessionBackend } from "../backends/native-backend-factory.js";
import type { NativeExecutionInput } from "../contracts/native-execution.js";
import { executeNativeSession } from "../native-session-runtime.js";

const sha = (value: string | Buffer) => `sha256:${createHash("sha256").update(value).digest("hex")}`;
const quote = (value: string) => `'${value.replaceAll("'", "'\"'\"'")}'`;
const kill = (pid: number) => { try { process.kill(-pid, "SIGKILL"); } catch (error) { if ((error as NodeJS.ErrnoException).code !== "ESRCH") throw error; } };
const alive = (pid: number) => {
  try { process.kill(pid, 0); } catch (error) { if ((error as NodeJS.ErrnoException).code === "ESRCH") return false; throw error; }
  try { return !execFileSync("ps", ["-p", String(pid), "-o", "stat="], { encoding: "utf8" }).trim().startsWith("Z"); }
  catch (error) { if ((error as { status?: number }).status === 1) return false; throw error; }
};

function qualifiedProfile(agent: "claude" | "codex") {
  const qualified = resolveQualifiedAcpxProfile(agent, "explicit-source-model");
  return { driverKind: qualified.driverKind, protocolVersion: qualified.protocolVersion, acpxVersion: qualified.acpxVersion,
    agent: qualified.agent, agentProfileVersion: qualified.agentProfileVersion,
    agentServerPackage: qualified.agentServerPackage, agentServerVersion: qualified.agentServerVersion,
    agentRuntimePackage: qualified.agentRuntimePackage, agentRuntimeVersion: qualified.agentRuntimeVersion,
    commandDigest: qualified.commandDigest };
}

let scenario = 0;
const durableState = async (root: string) => (await Promise.all(["control-plane/control-plane-state.json", "runner/runner-state.json"]
  .map(path => readFile(join(root, path), "utf8").catch(() => "")))).join("\n");

async function runProviderTurnScenario(tokens: string, options: { agent?: "claude" | "codex"; capMs?: number; providerStartedTurns?: boolean } = {}) {
  const agent = options.agent ?? "claude";
  const providerStartedTurns = options.providerStartedTurns ?? agent === "claude";
  const turnStarts: Array<Record<string, unknown>> = [];
  const notificationMethods: string[] = [];
  const diagnostics: string[] = [];
  const n = ++scenario;
  const root = await realpath(await mkdtemp(join(tmpdir(), "runner-acpx-provider-turn-")));
  const journal = join(root, "calls"), fault = join(root, "fault"), wrapper = join(root, "sidecar"), script = join(root, "sidecar-entry");
  const background = join(root, "background");
  const binary = defaultCapabilityRunnerdBinary(), sidecar = resolve(import.meta.dirname, "../../runner/target/debug/fake-acpx-sidecar");
  await mkdir(join(root, "acpx"), { mode: 0o700 }); await mkdir(join(root, "diagnostics"), { mode: 0o700 });
  await writeFile(script, "qualified fixture entry");
  await writeFile(background, tokens);
  await writeFile(wrapper, `#!/bin/sh\nexec ${quote(sidecar)} --mode source-reconcile --profile-digest ${quote(QUALIFIED_ACPX_PROFILES[agent].commandDigest)} --journal ${quote(journal)} --source-fault ${quote(fault)} --background-tasks ${quote(background)} --spawn-owned-child\n`, { mode: 0o700 });
  let authority: DurablePrpControlPlane | null = null;
  const server = createServer();
  server.on("upgrade", (request, socket, head) => authority ? authority.handleUpgrade(request, socket, "/runner", head) : socket.destroy());
  await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
  const address = server.address(); if (!address || typeof address === "string") throw new Error("missing fixture listener");
  const identity = { runnerInstanceId: `runner-provider-turn-${n}`, environmentLeaseId: `lease-provider-turn-${n}`, runId: `run-provider-turn-${n}`,
    normalizedSessionId: `session-provider-turn-${n}`, turnId: `turn-provider-turn-${n}`, itemId: `item-provider-turn-${n}` };
  const nativeIdentity = { companyId: `company-${n}`, agentId: "agent", issueId: "issue", runId: identity.runId, sessionId: identity.normalizedSessionId };
  const execution: NativeExecutionInput = { schema: "paperclip.native-execution-input.v1",
    binding: { companyId: nativeIdentity.companyId, agentId: nativeIdentity.agentId, issueId: nativeIdentity.issueId, runId: identity.runId, executionWorkspaceId: "workspace" },
    task: { identifier: "TASK-1", title: "Install and test", description: null, prompt: "Install and test", workMode: "standard" },
    workspace: { cwd: root, repoUrl: null, repoRef: null, branchName: null },
    session: { normalizedSessionId: identity.normalizedSessionId, driverKind: "acpx_runtime", protocolVersion: 1, lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null } },
    provider: { kind: "acpx", agent, model: "explicit-source-model", permissionPolicy: "interactive", profile: qualifiedProfile(agent) },
    completionContract: { id: "contract", sha256: sha("contract"), schemaVersion: "paperclip.completion-contract.v1",
      contract: { revision: "acpx-provider-turns-v1", objective: "Install and test", criteria: [{ id: "objective", requirement: "Install and test" }] } },
    interactionResponses: [], credentialBindings: [] };
  const runnerPids = new Set<number>();
  const bundle = createCapabilityRunnerdCodexTransport({
    provider: "acpx", acpxAgent: agent, ...(providerStartedTurns ? { providerStartedTurns: true } : {}),
    onDiagnostic: message => diagnostics.push(message), runnerBinary: binary, stateDirectory: root, prpIdentity: identity,
    runnerArtifact: readRunnerdArtifactBinding(binary), codexSourceRecoveryVersion: 1, acpxSourceRecoveryVersion: 1,
    lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null }, runnerProcessReconnectGraceMs: null, runnerReconnectGraceMs: 3_000,
    controlPlaneRegistration: async next => {
      authority = next;
      const queueCommand = next.queueCommand.bind(next);
      next.queueCommand = (type, payload, ...rest) => {
        if (type === "turn.start") turnStarts.push(structuredClone(payload ?? {}));
        return queueCommand(type, payload, ...rest);
      };
      return { connectUrl: `ws://127.0.0.1:${address.port}/runner`, release: () => { authority = null; } }; },
    prepareLaunch: async launch => ({ configurationId: `initial-${launch.identity.runId}`, requirementsDigest: runnerLaunchRequirementsDigest(launch),
      provider: createRunnerProviderLaunch(launch, { options: {}, root, cwd: root, codexHome: root, codexCommand: "", providerNodeCommand: wrapper, opencodeProxyPath: "", acpxSidecarPath: script }) }),
    runnerProcessLauncher: spec => {
      const stderr = openSync(join(root, "native.stderr"), "a", 0o600);
      const child = spawn(binary, [...spec.args, "--acpx-launch-authority-digest", sha("fixture authority"),
        "--acpx-sidecar-command", wrapper, "--acpx-sidecar-command-sha256", sha(execFileSync("cat", [wrapper])),
        "--acpx-sidecar-script", script, "--acpx-sidecar-script-sha256", sha("qualified fixture entry")],
        { env: { PATH: process.env.PATH, HOME: root, ...spec.environment }, stdio: ["ignore", "ignore", stderr], detached: true });
      closeSync(stderr);
      if (child.pid) runnerPids.add(child.pid);
      return { child, completion: new Promise((done, reject) => { child.once("error", reject); child.once("exit", (code, signal) => done({ code, signal, stdout: "", stderr: "" })); }) };
    },
  });
  const notifications = bundle.transport.notifications.bind(bundle.transport);
  bundle.transport.notifications = () => ({ async *[Symbol.asyncIterator]() {
    for await (const notification of notifications()) { notificationMethods.push(notification.method); yield notification; }
  } });
  const calls = async () => (await readFile(journal, "utf8").catch(() => "")).trim().split("\n").filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
  const appended: PrpEvent[] = [];
  const completeRun = vi.fn(async () => undefined);
  const resolveMissingResult = vi.fn(async () => null);
  try {
    const backend = createNativeSessionBackend(execution, { codexTransportFactory: () => bundle.transport,
      environment: { PAPERCLIP_WORKSPACE_CWD: root }, dynamicTools: [] });
    const started = Date.now();
    const outcome = await executeNativeSession({
      input: execution, backend, runnerInstanceId: identity.runnerInstanceId, controlPlaneInstanceId: "control-provider-turn",
      controlPlane: {
        async openRun() {}, async checkpointSession() {},
        async appendEvent(value: PrpEvent) {
          appended.push(structuredClone(value));
          return { cursor: value.sourceSeq, highestContiguousSourceSeq: value.sourceSeq, disposition: "committed" };
        },
        async replayEvents() { return { events: [], highestContiguousSourceSeq: 0 }; },
        completeRun,
      },
      timeoutMs: 30_000, resolveMissingResult,
      providerTurnWait: options.capMs === undefined ? undefined : { capMs: options.capMs },
    }).then(() => "finalized", (error: unknown) => String(error instanceof Error ? error.message : error));
    const elapsedMs = Date.now() - started;
    const closeOutcome = await bundle.transport.close().then(() => "closed", (error: unknown) => String(error instanceof Error ? error.message : error));
    const durable = await durableState(root);
    const journalled = await calls();
    const providerPids = journalled.flatMap(entry => [entry.processStart, entry.childPid]).filter((pid): pid is number => typeof pid === "number");
    await vi.waitFor(() => { for (const pid of providerPids) expect(alive(pid)).toBe(false); }, { timeout: 5_000, interval: 50 }).catch(() => undefined);
    const providerProcesses = providerPids.map(pid => ({ pid, exited: !alive(pid) }));
    return { outcome, appended, completeRun, resolveMissingResult, journalled, providerProcesses, root, elapsedMs, turnStarts, notificationMethods, closeOutcome, diagnostics, durable };
  } catch (error) {
    const diagnostics = (await readFile(join(root, "native.stderr"), "utf8").catch(() => ""))
      + (await readFile(join(root, "diagnostics", "runnerd.stderr.log"), "utf8").catch(() => ""));
    throw new Error(`${String(error)}\n${diagnostics}`, { cause: error });
  } finally {
    await bundle.transport.close().catch(() => undefined);
    for (const pid of runnerPids) kill(pid);
    for (const entry of await calls()) if (entry.processStart) kill(entry.processStart);
    server.closeAllConnections(); await new Promise<void>(done => server.close(() => done()));
    await rm(root, { recursive: true, force: true });
  }
}

const sidecarCommands = (journalled: Array<Record<string, unknown>>) => journalled.filter(entry => typeof entry.command === "string").map(entry => entry.command);
const unsettled = (messages: string[]) => messages.filter(message => message.startsWith("native_session_settlement_incomplete"));
const expectCleanClose = (run: Awaited<ReturnType<typeof runProviderTurnScenario>>) => {
  expect(run.closeOutcome).toBe("closed");
  expect(unsettled(run.diagnostics)).toEqual([]);
  expect(run.durable).not.toContain("conflicting duplicate");
};

it.skipIf(process.platform === "win32")("continues a run in the provider's own turn after background work and settles its result", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-result-resolved");
  expect(run.outcome).toBe("finalized");
  expect(run.completeRun).toHaveBeenCalledOnce();
  expect(run.resolveMissingResult).not.toHaveBeenCalled();
  const awaited = run.appended.find(event => event.eventType === "turn.completed" && event.payload.awaitingProviderTurn === true);
  expect(awaited).toBeDefined();
  const started = run.appended.filter(event => event.eventType === "turn.started");
  expect(started).toHaveLength(2);
  expect(started[1]).toMatchObject({ turnId: "provider-1", payload: { origin: "provider" } });
  expect(run.appended.filter(event => event.payload.kind === "usage").map(event => event.turnId)).toContain("provider-1");
  expect(sidecarCommands(run.journalled)).toContain("session.provider_turn");
  expect(run.turnStarts).toEqual([expect.objectContaining({ providerStartedTurns: true })]);
  expect(run.notificationMethods).toContain("paperclip/runTerminal");
  expect(run.appended.filter(event => event.eventType === "run.result.accepted")).toHaveLength(1);
  expect(run.appended.filter(event => event.eventType === "run.result.proposed").map(event => event.turnId)).toEqual(["provider-1"]);
  expect(run.appended.filter(event => event.eventType === "turn.completed").map(event => event.turnId)).toContain("provider-1");
  const resolved = run.journalled.filter(entry => entry.command === "tool.resolve");
  expect(resolved.map(entry => [entry.params.turnId, entry.params.callId, entry.params.error ?? null])).toEqual([["provider-1", "call-provider-finish", null]]);
  expectCleanClose(run);
}, 60_000);

it.skipIf(process.platform === "win32")("keeps a Claude run on today's path when the caller does not ask for provider-started turns", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-result", { providerStartedTurns: false });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.turnStarts).toHaveLength(1);
  expect(run.turnStarts[0]).not.toHaveProperty("providerStartedTurns");
  expect(run.notificationMethods).not.toContain("paperclip/runTerminal");
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(false);
  expect(run.appended.some(event => event.payload.origin === "provider")).toBe(false);
  expect(sidecarCommands(run.journalled)).not.toContain("session.provider_turn");
  expectCleanClose(run);
}, 60_000);

it.skipIf(process.platform === "win32")("ends the wait at the cap and takes today's missing-result path when the provider never starts its turn", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-turn-hold", { capMs: 300 });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.completeRun).not.toHaveBeenCalled();
  expect(run.resolveMissingResult).toHaveBeenCalledWith(expect.objectContaining({
    terminalEvent: expect.objectContaining({ eventType: "turn.completed", payload: expect.objectContaining({ awaitingProviderTurn: true }) }) }));
  expect(run.appended.some(event => event.payload.status === "provider_turn_wait_ended")).toBe(true);
  expect(run.appended.some(event => event.payload.origin === "provider")).toBe(false);
  expect(run.providerProcesses.length).toBeGreaterThan(1);
  expect(run.providerProcesses.filter(entry => !entry.exited)).toEqual([]);
  expectCleanClose(run);
}, 60_000);

it.skipIf(process.platform === "win32")("ends the wait in seconds, not at the cap, when the provider process is lost while awaiting", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-turn-hold provider-lost-after-arm", { capMs: 30_000 });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.completeRun).not.toHaveBeenCalled();
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(true);
  expect(run.elapsedMs).toBeLessThan(15_000);
  expectCleanClose(run);
}, 60_000);

it.skipIf(process.platform === "win32")("ends the wait in seconds, not at the cap, when the sidecar exits while awaiting", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-turn-hold provider-sidecar-exit-after-arm", { capMs: 30_000 });
  expect(run.completeRun).not.toHaveBeenCalled();
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(true);
  expect(run.elapsedMs).toBeLessThan(15_000);
  expect(run.closeOutcome).toBe("provider_transport_failed: runner did not durably suspend before checkpoint");
  expect(unsettled(run.diagnostics)).toEqual([expect.stringContaining('"proofWindowEnd":"suspend_not_completed"')]);
  expect(run.durable).not.toContain("conflicting duplicate");
}, 60_000);

it.skipIf(process.platform === "win32")("leaves a Claude turn without background work exactly as before", async () => {
  const run = await runProviderTurnScenario("quiet provider-turn");
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(false);
  expect(run.appended.some(event => event.payload.status === "provider_turn_wait_ended")).toBe(false);
  expect(sidecarCommands(run.journalled)).not.toContain("session.provider_turn");
  expectCleanClose(run);
}, 60_000);

it.skipIf(process.platform === "win32")("never opts a non-Claude ACPX agent in", async () => {
  const run = await runProviderTurnScenario("live provider-turn provider-result", { agent: "codex", providerStartedTurns: true });
  expect(run.outcome).toContain("native_finalization_missing");
  expect(run.turnStarts[0]).not.toHaveProperty("providerStartedTurns");
  expect(run.notificationMethods).not.toContain("paperclip/runTerminal");
  expect(run.appended.some(event => event.payload.awaitingProviderTurn === true)).toBe(false);
  expect(sidecarCommands(run.journalled)).not.toContain("session.provider_turn");
  expectCleanClose(run);
}, 60_000);

async function runRestartScenario(tokens: string, options: { waitMs: number; lastTurnId: string; resumeTokens?: string }) {
  const n = ++scenario;
  const root = await realpath(await mkdtemp(join(tmpdir(), "runner-acpx-provider-turn-restart-")));
  const journal = join(root, "calls"), fault = join(root, "fault"), wrapper = join(root, "sidecar"), script = join(root, "sidecar-entry");
  const background = join(root, "background");
  const binary = defaultCapabilityRunnerdBinary(), sidecar = resolve(import.meta.dirname, "../../runner/target/debug/fake-acpx-sidecar");
  await mkdir(join(root, "acpx"), { mode: 0o700 }); await mkdir(join(root, "diagnostics"), { mode: 0o700 });
  await writeFile(script, "qualified fixture entry");
  await writeFile(background, tokens);
  await writeFile(wrapper, `#!/bin/sh\nexec ${quote(sidecar)} --mode source-reconcile --profile-digest ${quote(QUALIFIED_ACPX_PROFILES.claude.commandDigest)} --journal ${quote(journal)} --source-fault ${quote(fault)} --background-tasks ${quote(background)} --spawn-owned-child\n`, { mode: 0o700 });
  let authority: DurablePrpControlPlane | null = null, registrations = 0;
  const diagnostics: string[] = [];
  const server = createServer();
  server.on("upgrade", (request, socket, head) => authority ? authority.handleUpgrade(request, socket, "/runner", head) : socket.destroy());
  await new Promise<void>(done => server.listen(0, "127.0.0.1", done));
  const address = server.address(); if (!address || typeof address === "string") throw new Error("missing fixture listener");
  const identity = { runnerInstanceId: `runner-provider-turn-restart-${n}`, environmentLeaseId: `lease-provider-turn-restart-${n}`, runId: `run-provider-turn-restart-${n}`,
    normalizedSessionId: `session-provider-turn-restart-${n}`, turnId: `turn-provider-turn-restart-${n}`, itemId: `item-provider-turn-restart-${n}` };
  const nativeIdentity = { companyId: `company-restart-${n}`, agentId: "agent", issueId: "issue", runId: identity.runId, sessionId: identity.normalizedSessionId };
  const execution: NativeExecutionInput = { schema: "paperclip.native-execution-input.v1",
    binding: { companyId: nativeIdentity.companyId, agentId: "agent", issueId: "issue", runId: identity.runId, executionWorkspaceId: "workspace" },
    task: { identifier: "TASK-1", title: "Install and test", description: null, prompt: "Install and test", workMode: "standard" },
    workspace: { cwd: root, repoUrl: null, repoRef: null, branchName: null },
    session: { normalizedSessionId: identity.normalizedSessionId, driverKind: "acpx_runtime", protocolVersion: 1, lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null } },
    provider: { kind: "acpx", agent: "claude", model: "explicit-source-model", permissionPolicy: "interactive", profile: qualifiedProfile("claude") },
    completionContract: { id: "contract", sha256: sha("contract"), schemaVersion: "paperclip.completion-contract.v1",
      contract: { revision: "acpx-provider-turns-v1", objective: "Install and test", criteria: [{ id: "objective", requirement: "Install and test" }] } },
    interactionResponses: [], credentialBindings: [] };
  const runnerPids = new Set<number>();
  let requirements: RunnerProviderLaunchRequirements | undefined;
  const transportOptions: CapabilityRunnerdCodexTransportOptions = {
    provider: "acpx", acpxAgent: "claude", providerStartedTurns: true, onDiagnostic: message => diagnostics.push(message), runnerBinary: binary, stateDirectory: root, prpIdentity: identity,
    runnerArtifact: readRunnerdArtifactBinding(binary), codexSourceRecoveryVersion: 1, acpxSourceRecoveryVersion: 1,
    lifecyclePolicy: { mode: "per_turn", idleTimeoutMs: null }, runnerProcessReconnectGraceMs: null, runnerReconnectGraceMs: 10_000,
    controlPlaneRegistration: async next => {
      const generation = ++registrations; authority = next;
      return { connectUrl: `ws://127.0.0.1:${address.port}/runner`, release: () => { if (generation === registrations) authority = null; } };
    },
    prepareLaunch: async launch => {
      requirements ??= structuredClone(launch);
      return { configurationId: `initial-${launch.identity.runId}`, requirementsDigest: runnerLaunchRequirementsDigest(launch),
        provider: createRunnerProviderLaunch(launch, { options: {}, root, cwd: root, codexHome: root, codexCommand: "", providerNodeCommand: wrapper, opencodeProxyPath: "", acpxSidecarPath: script }) };
    },
    runnerProcessLauncher: spec => {
      const stderr = openSync(join(root, "native.stderr"), "a", 0o600);
      const child = spawn(binary, [...spec.args, "--acpx-launch-authority-digest", sha("fixture authority"),
        "--acpx-sidecar-command", wrapper, "--acpx-sidecar-command-sha256", sha(execFileSync("cat", [wrapper])),
        "--acpx-sidecar-script", script, "--acpx-sidecar-script-sha256", sha("qualified fixture entry")],
        { env: { PATH: process.env.PATH, HOME: root, ...spec.environment }, stdio: ["ignore", "ignore", stderr], detached: true });
      closeSync(stderr);
      if (child.pid) runnerPids.add(child.pid);
      return { child, completion: new Promise((done, reject) => { child.once("error", reject); child.once("exit", (code, signal) => done({ code, signal, stdout: "", stderr: "" })); }) };
    },
  };
  const calls = async () => (await readFile(journal, "utf8").catch(() => "")).trim().split("\n").filter(Boolean)
    .flatMap(line => { try { return [JSON.parse(line)]; } catch { return []; } });
  const backend = (transport: CapabilityRunnerdCodexTransport) => createNativeSessionBackend(execution, { codexTransportFactory: () => transport.transport,
    environment: { PAPERCLIP_WORKSPACE_CWD: root }, dynamicTools: [] });
  try {
    const first = createCapabilityRunnerdCodexTransport(transportOptions);
    const session = await backend(first).openSession({ identity: nativeIdentity, workingDirectory: root });
    await session.startTurn({ message: { role: "user", text: "Install and test." } });
    for await (const event of session.events()) if (event.eventType === "turn.completed") break;
    const checkpoint = await session.snapshot();
    const pid = first.evidence().runnerPid!, startedAt = first.transport.processInfo!().startedAt;
    await session.detachControllerForRestart!();
    await new Promise(done => setTimeout(done, options.waitMs));
    const atRestart = { journalled: await calls(), durable: await durableState(root) };
    const second = createCapabilityRunnerdCodexTransport({ ...transportOptions, recoveryLaunch: { requirements: requirements! },
      resumeProviderSession: { driverSessionId: checkpoint.sessionId, providerSessionId: checkpoint.providerSessionId!, providerIdentity: checkpoint.providerIdentity },
      authorityRunId: identity.runId, runnerProcessLauncher: () => { throw new Error("duplicate runner launch"); },
      adoptExistingRunner: { pid, processGroupId: pid, startedAt, isAlive: () => alive(pid), signal: () => false } });
    const serverRequests: Array<Record<string, any>> = [];
    const install = second.transport.setServerRequestHandler.bind(second.transport);
    second.transport.setServerRequestHandler = handler => install(async request => {
      const response = await handler(request);
      serverRequests.push(structuredClone({ method: request.method, params: request.params, response }));
      return response;
    });
    const recovery = await backend(second).recoverSession!(checkpoint, { signal: new AbortController().signal });
    const result = { checkpoint, pid, recoveredPid: second.evidence().runnerPid, recovered: recovery.recovered, reason: recovery.reason, atRestart, serverRequests,
      delivered: [] as PrpEvent[], snapshot: null as unknown, closeOutcome: "not closed", journalled: [] as Array<Record<string, any>>, diagnostics, durable: "" };
    if (recovery.session) {
      if (options.resumeTokens !== undefined) await writeFile(background, options.resumeTokens);
      const events = recovery.session.events()[Symbol.asyncIterator]();
      const timeout = new Promise<null>(done => setTimeout(() => done(null), 15_000));
      for (let next = await Promise.race([events.next(), timeout]); next && !next.done; next = await Promise.race([events.next(), timeout])) {
        result.delivered.push(next.value);
        if (next.value.eventType === "session.failed" || (next.value.eventType === "turn.completed" && next.value.turnId === options.lastTurnId)) break;
      }
      result.snapshot = await recovery.session.snapshot();
      result.closeOutcome = await recovery.session.close({ reason: "fixture complete" }).then(() => "closed", (error: unknown) => String(error instanceof Error ? error.message : error));
    }
    return { ...result, journalled: await calls(), durable: await durableState(root) };
  } catch (error) {
    throw new Error(`${String(error)}\n${await readFile(join(root, "native.stderr"), "utf8").catch(() => "")}`, { cause: error });
  } finally {
    for (const pid of runnerPids) kill(pid);
    for (const entry of await calls()) if (entry.processStart) kill(entry.processStart);
    server.closeAllConnections(); await new Promise<void>(done => server.close(() => done()));
    await rm(root, { recursive: true, force: true });
  }
}

type RestartRun = Awaited<ReturnType<typeof runRestartScenario>>;
const ofType = (run: RestartRun, eventType: string) => run.delivered.filter(event => event.eventType === eventType);
const resolvedCalls = (journalled: Array<Record<string, any>>) => journalled.filter(entry => entry.command === "tool.resolve")
  .map(entry => [entry.params.turnId, entry.params.callId, entry.params.error ?? null]);
const armedWaits = (journalled: Array<Record<string, any>>) => journalled.filter(entry => entry.command === "session.provider_turn" && entry.params.armed === true).length;
const expectRecoveredCleanly = (run: RestartRun) => {
  expect(run.recovered, run.reason).toBe(true);
  expect(run.recoveredPid).toBe(run.pid);
  expect(ofType(run, "session.failed")).toEqual([]);
  expect(run.closeOutcome).toBe("closed");
  expect(unsettled(run.diagnostics)).toEqual([]);
  expect(run.durable).not.toContain("conflicting duplicate");
};

it.skipIf(process.platform === "win32").each([true, false])("delivers the provider's own turn that ran while the controller was away, after a restart during the wait (result: %s)", async withResult => {
  const run = await runRestartScenario(`live provider-turn ${withResult ? "provider-result-resolved " : ""}provider-goal-at-terminal provider-turn-delay-ms=1500`, { waitMs: 2_500, lastTurnId: "provider-1" });
  expect(run.checkpoint).toMatchObject({ activeTurnId: null, semanticResult: null, awaitingProviderTurn: true });
  expectRecoveredCleanly(run);
  expect(ofType(run, "turn.started").map(event => [event.turnId, event.payload.origin])).toEqual([["provider-1", "provider"]]);
  expect(ofType(run, "item.completed").filter(event => event.payload.kind === "agentMessage").map(event => event.turnId)).toEqual(["provider-1"]);
  expect(run.delivered.filter(event => event.payload.kind === "usage").map(event => event.turnId)).toEqual(["provider-1"]);
  if (withResult) {
    expect(ofType(run, "run.result.proposed").map(event => event.turnId)).toEqual(["provider-1"]);
    expect(run.snapshot).toMatchObject({ semanticResult: { reportedWorkDisposition: "done" }, terminal: { turnTerminalState: "completed", runTerminalState: "succeeded" } });
    expect(resolvedCalls(run.journalled)).toEqual([["provider-1", "call-provider-finish", null]]);
  } else expect(ofType(run, "run.result.proposed")).toEqual([]);
}, 60_000);

it.skipIf(process.platform === "win32")("delivers and answers the finish call of a provider turn that was still waiting for it at a restart", async () => {
  const run = await runRestartScenario("live provider-turn provider-result-resolved provider-goal-at-terminal provider-turn-delay-ms=1500", { waitMs: 2_500, lastTurnId: "provider-1" });
  expect(armedWaits(run.atRestart.journalled)).toBe(1);
  expect(resolvedCalls(run.atRestart.journalled)).toEqual([]);
  expect(run.atRestart.durable).toContain("call-provider-finish");
  expectRecoveredCleanly(run);
  expect(ofType(run, "turn.started").map(event => [event.turnId, event.payload.origin])).toEqual([["provider-1", "provider"]]);
  expect(run.serverRequests.map(request => [request.params.turnId, request.params.tool, request.response.success])).toEqual([["provider-1", "paperclip_finish", true]]);
  expect(resolvedCalls(run.journalled)).toEqual([["provider-1", "call-provider-finish", null]]);
  expect(ofType(run, "turn.completed").map(event => event.turnId)).toEqual(["provider-1"]);
  expect(ofType(run, "run.result.proposed").map(event => event.turnId)).toEqual(["provider-1"]);
  expect(run.snapshot).toMatchObject({ activeTurnId: null, semanticResult: { reportedWorkDisposition: "done" }, terminal: { turnTerminalState: "completed", runTerminalState: "succeeded" } });
}, 60_000);

it.skipIf(process.platform === "win32")("delivers the rest of a provider turn that was still running at a restart once, in order", async () => {
  const tokens = "live provider-turn provider-goal-at-terminal provider-turn-delay-ms=1500";
  const run = await runRestartScenario(`${tokens} provider-turn-pause`, { waitMs: 2_500, lastTurnId: "provider-1", resumeTokens: tokens });
  expect(run.atRestart.durable).toContain("Claude is still working.");
  expect(run.atRestart.durable).not.toContain("Claude finished its own work.");
  expectRecoveredCleanly(run);
  const turn = run.delivered.filter(event => event.turnId === "provider-1");
  expect(turn.map(event => [event.eventType, event.payload.kind ?? null])).toEqual([["turn.started", null], ["item.delta", "agentMessage"], ["item.delta", "agentMessage"],
    ["item.completed", "usage"], ["item.completed", "agentMessage"], ["turn.completed", null]]);
  expect(turn[0]!.payload.origin).toBe("provider");
  const sequence = run.delivered.map(event => event.sourceSeq);
  expect(sequence).toEqual([...new Set(sequence)].sort((left, right) => left - right));
  expect(ofType(run, "run.result.proposed")).toEqual([]);
  expect(run.serverRequests).toEqual([]);
  expect(resolvedCalls(run.journalled)).toEqual([]);
}, 60_000);

it.skipIf(process.platform === "win32")("delivers two provider turns that the runner started while the controller was away, in order", async () => {
  const run = await runRestartScenario("live provider-turn provider-turns=live/resolved provider-goal-at-terminal provider-turn-delay-ms=1000", { waitMs: 3_500, lastTurnId: "provider-2" });
  expect(run.atRestart.journalled.filter(entry => entry.command === "session.provider_turn").map(entry => [entry.params.turnId, entry.params.armed]))
    .toEqual([[expect.any(String), true], ["provider-1", true]]);
  expect(resolvedCalls(run.atRestart.journalled)).toEqual([]);
  expectRecoveredCleanly(run);
  expect(ofType(run, "turn.started").map(event => [event.turnId, event.payload.origin])).toEqual([["provider-1", "provider"], ["provider-2", "provider"]]);
  expect(ofType(run, "turn.completed").map(event => [event.turnId, event.payload.awaitingProviderTurn ?? false])).toEqual([["provider-1", true], ["provider-2", false]]);
  expect(ofType(run, "item.completed").filter(event => event.payload.kind === "agentMessage").map(event => event.turnId)).toEqual(["provider-1", "provider-2"]);
  expect(run.delivered.filter(event => event.payload.kind === "usage").map(event => event.turnId)).toEqual(["provider-1", "provider-2"]);
  expect(ofType(run, "run.result.proposed").map(event => event.turnId)).toEqual(["provider-2"]);
  expect(run.serverRequests.map(request => [request.params.turnId, request.params.tool, request.response.success])).toEqual([["provider-2", "paperclip_finish", true]]);
  expect(resolvedCalls(run.journalled)).toEqual([["provider-2", "call-provider-finish", null]]);
  expect(run.snapshot).toMatchObject({ semanticResult: { reportedWorkDisposition: "done" }, terminal: { turnTerminalState: "completed", runTerminalState: "succeeded" } });
}, 60_000);
