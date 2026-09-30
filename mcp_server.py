"""MCP Server for Agent Mesh State Blackboard Deconfliction Engine."""
import sys
import json
import time
from client import AgentMeshStateBlackboardDeconflictionEngine

blackboard = AgentMeshStateBlackboardDeconflictionEngine()

def handle_call_tool(params):
    name = params.get("name")
    args = params.get("arguments", {})
    if name != "manage_mesh_blackboard_state":
        raise ValueError(f"Unknown tool: {name}")

    action = args.get("action", "get_blackboard_telemetry")
    if action == "propose_state_mutation":
        return blackboard.propose_state_mutation(
            agent_id=args.get("agent_id", "agent_1"),
            state_key=args.get("state_key", "var"),
            proposed_value=args.get("proposed_value"),
            expected_version=args.get("expected_version")
        )
    elif action == "read_shared_state":
        return blackboard.read_shared_state(
            state_key=args.get("state_key", "")
        )
    elif action == "acquire_resource_lock":
        return blackboard.acquire_resource_lock(
            agent_id=args.get("agent_id", "agent_1"),
            lock_resource_id=args.get("lock_resource_id", "res_1"),
            lease_seconds=args.get("lease_seconds")
        )
    elif action == "release_resource_lock":
        return blackboard.release_resource_lock(
            agent_id=args.get("agent_id", "agent_1"),
            lock_resource_id=args.get("lock_resource_id", "res_1")
        )
    elif action == "get_blackboard_telemetry":
        return blackboard.get_blackboard_telemetry()
    else:
        raise ValueError(f"Invalid action: {action}")

def main():
    if len(sys.argv) > 1 and sys.argv[1] == "--test":
        print("Running self-test...")
        lock = blackboard.acquire_resource_lock("w1", "flight_seat_12A", 10.0)
        assert lock["locked"] is True
        mut = blackboard.propose_state_mutation("w1", "seat_assignment", "CONFIRMED_12A")
        assert mut["accepted"] is True
        # Contention test
        cont = blackboard.acquire_resource_lock("w2", "flight_seat_12A", 10.0)
        assert cont["locked"] is False
        print("Self-test PASSED!")
        sys.exit(0)

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
            msg_id = req.get("id")
            method = req.get("method")
            if method == "initialize":
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "protocolVersion": "2024-11-05",
                        "serverInfo": {"name": "AgentMeshStateBlackboardDeconflictionEngine", "version": "1.0.0"},
                        "capabilities": {"tools": {}}
                    }
                }
            elif method == "tools/list":
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {
                        "tools": [{
                            "name": "manage_mesh_blackboard_state",
                            "description": "Multi-agent blackboard coordination: propose state mutations, detect concurrent write collisions, synchronize vector clocks, and arbitrate competing agent goals.",
                            "inputSchema": {
                                "type": "object",
                                "properties": {
                                    "action": {"type": "string", "enum": ["propose_state_mutation", "read_shared_state", "acquire_resource_lock", "release_resource_lock", "get_blackboard_telemetry"]},
                                    "agent_id": {"type": "string"},
                                    "state_key": {"type": "string"},
                                    "proposed_value": {"type": "object"},
                                    "expected_version": {"type": "integer"},
                                    "lock_resource_id": {"type": "string"},
                                    "lease_seconds": {"type": "number"}
                                },
                                "required": ["action"]
                            }
                        }]
                    }
                }
            elif method == "tools/call":
                res = handle_call_tool(req.get("params", {}))
                resp = {
                    "jsonrpc": "2.0",
                    "id": msg_id,
                    "result": {"content": [{"type": "text", "text": json.dumps(res, indent=2)}]}
                }
            else:
                resp = {"jsonrpc": "2.0", "id": msg_id, "result": {}}
            print(json.dumps(resp), flush=True)
        except Exception as e:
            err_resp = {"jsonrpc": "2.0", "id": None, "error": {"code": -32000, "message": str(e)}}
            print(json.dumps(err_resp), flush=True)

if __name__ == "__main__":
    main()
