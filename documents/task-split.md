# DDS Group Project - Task Split (4 Members)
**Revised: Feb 22 - Dual Protocol Implementation (2PC & SAGA)**

## Deadlines & Milestones
| Milestone | Date | Status |
| :--- | :--- | :--- |
| Group Formation | Feb 16 | Complete |
| **Phase 1: Dual Protocol Impl** | **Mar 13** | In Progress |
| **Phase 2: Orchestrator Refactor** | **Apr 01** | Planned |

---

## Riya: The Saga Lead (Order Logic & Workflow)
**Focus:** High-level coordination, state machine logic, and compensating transactions.

### Phase 1
* **Saga Orchestrator:** Implement the checkout flow in `order/app.py` using the Saga pattern (Orchestration-based).
* **State Management:** Define the "Forward" (Reserve Stock -> Deduct Payment) and "Backward" (Refund Payment -> Release Stock) transitions.
* **Idempotency:** Ensure all Order endpoints can handle retries from the benchmark tool without double-processing.
* **Consistency Check:** Verify that money and stock counts return to original values after a failed Saga step.

### Phase 2
* **Library Integration:** Move Saga logic into the standalone Orchestrator artifact.
* **Declarative API:** Define the checkout as a "Recipe" or "Workflow" that the Orchestrator executes.

---

## Atharva: The 2PC Lead (Protocol & Atomic Ops)
**Focus:** Two-Phase Commit implementation, Redis atomicity, and participant hooks.

### Phase 1
* **2PC Protocol:** Implement the "Prepare" and "Commit/Abort" phases in the Order service (Coordinator) and Stock/Payment services (Participants).
* **Isolation/Locking:** Implement "Soft-reservations" or "Pending" states in Redis so items are held during the 'Prepare' phase but not finalized until 'Commit'.
* **Hardened Services:** Use Redis WATCH/MULTI/EXEC to ensure local participant actions (like updating credit) are truly atomic.

### Phase 2
* **Generic Interface:** Standardize the Participant API so the Orchestrator can toggle between 2PC and Saga modes for the same request.
* **Orchestrator Logic:** Assist Member A in integrating the 2PC Coordinator logic into the final artifact.

---

## Pranav: The Guardian (Fault Tolerance & Recovery)
**Focus:** Write-Ahead Logs (WAL), crash recovery, and data integrity.

### Phase 1
* **Transaction Logging:** Implement a persistent log in Redis to track the status of every in-flight 2PC and Saga transaction.
* **Recovery Manager:** Build a service-start routine that scans the log to resolve "hanging" transactions (e.g., if the Coordinator died mid-2PC).
* **Failure Handling:** Ensure the system recovers when a single container is killed as per the benchmark requirements.

### Phase 2
* **Stateful Orchestrator:** Ensure the new Orchestrator artifact is "Resume-capable"—if the Orchestrator pod dies, a new one picks up the log and finishes the job.

---

## Jimmy: The Architect (Infrastructure & Performance)
**Focus:** Kubernetes, Benchmarking, Chaos Testing, and the Orchestrator Wrapper.

### Phase 1
* **K8s & Persistence:** Setup manifests with resource limits and configure Redis AOF (Append Only File) to prevent data loss on DB restart.
* **Chaos Testing:** Create scripts to kill containers at specific moments (e.g., after a 2PC Prepare but before Commit) to verify Member C's logic.
* **Performance:** Run Locust and the wdm-benchmark to compare 2PC latency versus Saga latency under load.

### Phase 2
* **Artifact Packaging:** Lead the creation of the standalone Python package for the Orchestrator.
* **Documentation:** Create the "Switch" mechanism to allow the project to run in either 2PC or Saga mode for final evaluation.

---

## Shared Responsibilities
* **Interview Prep:** All members must understand the logic of both 2PC and Sagas and why one might be chosen over the other.
* **Code Review:** All PRs related to consistency must be reviewed by at least 2 members.
* **Zero Down-time:** Collaborative effort to ensure Kubernetes handles rolling updates and restarts without failing the benchmark.

---

## Technical Comparison


* **2PC:** Focuses on strong consistency (Atomicity). Requires participants to "Vote" and lock resources until the final decision.
* **Sagas:** Focuses on high availability (Eventual Consistency). Uses compensating transactions to undo steps if a later step fails.


- Confirm if Redis can be used as a DB. Is citus a good choice?
-
