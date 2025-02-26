#!/usr/bin/env python3
import os
import sys
import time
import argparse
import requests
import threading
import logging
from dotenv import load_dotenv
import json
from datetime import datetime, timedelta

# Load environment variables from .env file
load_dotenv()

# Get API key from environment variables
api_key = os.getenv("C3_API_KEY")

if not api_key:
    print("❌ Error: C3_API_KEY not found in .env file")
    print("Please create a .env file with your API key: C3_API_KEY=your_key_here")
    sys.exit(1)

# Get polling interval from environment or use default
WORKLOAD_POLL = int(os.getenv("WORKLOAD_POLL", "30"))  # Default: 30 seconds

# Base API endpoint
base_url = "https://api.comput3.ai/api/v0"

# Headers
headers = {
    "X-C3-API-KEY": api_key,
    "Content-Type": "application/json",
    "Origin": "https://launch.comput3.ai"
}

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__)

# Global node tracking
active_nodes = []
node_failures = {}  # Track failure counts per node
node_threads = {}   # Track monitoring threads per node
should_monitor = True


def get_running_workloads():
    """Get all currently running workloads"""
    url = f"{base_url}/workloads"
    data = {"running": True}

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"❌ Error getting workloads: {response.status_code}")
        logger.error(response.text)
        return []


def launch_workload(workload_type="ollama_webui:fast"):
    """Launch a new workload of the specified type"""
    url = f"{base_url}/launch"

    # Always set expiration to current time + 3600 seconds (1 hour)
    current_time = int(time.time())
    expires = current_time + 3600

    # Create launch data
    data = {
        "type": workload_type,
        "expires": expires
    }

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        result = response.json()
        result["type"] = workload_type  # Add type to result for easier tracking
        result["expires"] = expires     # Add expiration for tracking
        return result
    else:
        logger.error(f"❌ Error launching workload: {response.status_code}")
        logger.error(response.text)
        return None


def stop_workload(workload_id):
    """Stop a running workload by its ID"""
    url = f"{base_url}/stop"

    data = {"workload": workload_id}

    response = requests.post(url, headers=headers, json=data)

    if response.status_code == 200:
        return response.json()
    else:
        logger.error(f"❌ Error stopping workload: {response.status_code}")
        logger.error(response.text)
        return None


def stop_all_workloads():
    """Stop all active workloads and log the results"""
    logger.info(f"🛑 Stopping all {len(active_nodes)} active workloads...")

    for node_info in active_nodes:
        node_hostname = node_info.get('node')
        node_id = node_info.get('workload')

        logger.info(f"🛑 Stopping {node_hostname} (ID: {node_id})...")
        result = stop_workload(node_id)

        if result:
            stopped_time = datetime.fromtimestamp(result.get('stopped')).strftime('%Y-%m-%d %H:%M:%S')
            refund = result.get('refund_amount', 0)
            logger.info(f"✅ Successfully stopped {node_hostname} at {stopped_time} (Refund: {refund})")
        else:
            logger.error(f"❌ Failed to stop {node_hostname}")

    logger.info("✅ All workloads stopped")


def check_node_health(node):
    """Check if a node is responding"""
    node_url = f"https://{node}"
    headers = {"X-C3-API-KEY": api_key}

    try:
        response = requests.get(node_url, headers=headers, timeout=5)
        return response.status_code == 200
    except (requests.RequestException, Exception) as e:
        logger.warning(f"🔍 Health check failed for {node}: {str(e)}")
        return False


def replace_node(failed_node_info):
    """Replace a failed node with a new one of the same type"""
    node_hostname = failed_node_info.get('node')
    node_id = failed_node_info.get('workload')
    node_type = failed_node_info.get('type')

    logger.info(f"🔄 Replacing failed node {node_hostname} (ID: {node_id})")

    # Stop the failed node first
    stop_result = stop_workload(node_id)
    if stop_result:
        logger.info(f"✅ Successfully stopped failed node {node_hostname}")
    else:
        logger.warning(f"⚠️ Failed to stop node {node_hostname}, but continuing replacement")

    # Launch a new node of the same type
    new_node = launch_workload(workload_type=node_type)

    if new_node:
        logger.info(f"✅ Successfully launched replacement node {new_node.get('node')}")

        # Update the active nodes list - remove the old node and add the new one
        global active_nodes
        active_nodes = [node for node in active_nodes if node.get('workload') != node_id]
        active_nodes.append(new_node)

        # Reset failure counter for the new node
        node_failures[new_node.get('node')] = 0

        # Start monitoring the new node
        start_node_monitoring(new_node)
    else:
        logger.error(f"❌ Failed to launch replacement node")


def monitor_node(node_info):
    """Monitor a single node in a dedicated thread"""
    global active_nodes

    node_hostname = node_info.get('node')
    node_id = node_info.get('workload')
    node_type = node_info.get('type', 'unknown')

    logger.info(f"🔎 Starting monitoring thread for node {node_hostname} ({node_type})")

    # Initial status check
    is_healthy = check_node_health(node_hostname)
    if is_healthy:
        logger.info(f"✅ Node {node_hostname} is up and running")
    else:
        logger.warning(f"⚠️ Initial health check failed for node {node_hostname}, will retry")

    while should_monitor and node_info in active_nodes:
        # Check if the node is still in workloads response
        current_workloads = get_running_workloads()
        workload_ids = [w.get('workload') for w in current_workloads]

        if node_id not in workload_ids:
            logger.warning(f"⚠️ Node {node_hostname} (ID: {node_id}) is no longer in workloads response, considering it removed")
            # Remove from active nodes
            active_nodes = [node for node in active_nodes if node.get('workload') != node_id]
            break

        # Node is still in workloads, check health with retries
        is_alive = False
        for retry in range(3):  # Try up to 3 times (initial + 2 retries)
            is_healthy = check_node_health(node_hostname)
            if is_healthy:
                is_alive = True
                break
            else:
                logger.warning(f"⚠️ Health check failed for {node_hostname} (attempt {retry+1}/3)")

        if not is_alive:
            logger.error(f"❌ Node {node_hostname} failed all health checks, replacing...")
            replace_node(node_info)
            break
        else:
            logger.info(f"✅ Node {node_hostname} is healthy")

        # Sleep before next check
        time.sleep(WORKLOAD_POLL)

    logger.info(f"🛑 Monitoring stopped for node {node_hostname}")


def start_node_monitoring(node_info):
    """Start a dedicated monitoring thread for a node"""
    node_hostname = node_info.get('node')

    # Create and start a monitoring thread for this node
    thread = threading.Thread(
        target=monitor_node,
        args=(node_info,),
        daemon=True,
        name=f"monitor-{node_hostname}"
    )
    thread.start()

    # Store the thread reference
    node_threads[node_hostname] = thread

    logger.info(f"🔍 Monitoring thread started for {node_hostname} (polling every {WORKLOAD_POLL} seconds)")
    return thread


def launch_nodes(num_nodes=1, keep_running=False, node_type="alternate"):
    """Launch the specified number of nodes"""
    global active_nodes

    logger.info(f"🚀 Launching {num_nodes} nodes (keep running: {keep_running}, type: {node_type})")
    logger.info(f"📊 Node health polling interval: {WORKLOAD_POLL} seconds")

    # Calculate expiration time (even if keep_running is True, we set expires to current + 3600)
    current_time = int(time.time())

    successful_launches = []

    for i in range(num_nodes):
        # Determine node type based on parameter
        if node_type == "alternate":
            workload_type = "ollama_webui:fast" if i % 2 == 0 else "ollama_webui:large"
        else:
            workload_type = f"ollama_webui:{node_type}"

        logger.info(f"🔄 Launching node {i+1}/{num_nodes} ({workload_type})...")
        result = launch_workload(workload_type=workload_type)

        if result:
            successful_launches.append(result)
            logger.info(f"✅ Node {i+1}: {result.get('node')} (ID: {result.get('workload')})")

            # Small delay to allow the node to boot up
            logger.info(f"⏳ Waiting 5 seconds for node to initialize...")
            time.sleep(5)

            # Initialize failure counter
            node_failures[result.get('node')] = 0
        else:
            logger.error(f"❌ Failed to launch node {i+1}")

    # Print summary
    logger.info("\n=== 📊 Launch Summary ===")
    logger.info(f"Requested: {num_nodes} nodes")
    logger.info(f"Successful: {len(successful_launches)} nodes")

    for idx, node in enumerate(successful_launches):
        node_type_str = node.get("type", "unknown")

        expiry_time = datetime.fromtimestamp(node.get('expires')).strftime('%Y-%m-%d %H:%M:%S')
        logger.info(f"{idx+1}. {node.get('node')} (Type: {node_type_str}, Expires: {expiry_time})")

    # Update active nodes
    active_nodes = successful_launches

    # Start monitoring threads for each node
    for node_info in active_nodes:
        start_node_monitoring(node_info)


def main():
    parser = argparse.ArgumentParser(description="Comput3.ai Workload Manager")
    parser.add_argument("--nodes", type=int, default=1, help="Number of nodes to launch (default: 1)")
    parser.add_argument("--runtime", type=int, help="Runtime in seconds (default: 3600)")
    parser.add_argument("--keep-running", action="store_true", help="Keep nodes running with hourly renewal")
    parser.add_argument("--poll", type=int, help="Node health check interval in seconds (default: 30)")
    parser.add_argument("--type", type=str, default="alternate", choices=["fast", "large", "alternate"],
                      help="Node type to launch (fast, large, or alternate) (default: alternate)")
    parser.add_argument("--stop-on-exit", action="store_true", help="Stop all workloads when the script exits")

    args = parser.parse_args()

    # Validate arguments
    if args.nodes < 1:
        logger.error("❌ Error: Number of nodes must be at least 1")
        sys.exit(1)

    # Update polling interval if specified
    if args.poll is not None and args.poll > 0:
        global WORKLOAD_POLL
        WORKLOAD_POLL = args.poll
        logger.info(f"📊 Setting node health polling interval to {WORKLOAD_POLL} seconds")

    # Launch nodes with specified parameters
    launch_nodes(args.nodes, args.keep_running, node_type=args.type)

    try:
        # Keep the script running to allow monitoring
        while any(thread.is_alive() for thread in node_threads.values()):
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("👋 Shutting down monitoring...")
        global should_monitor
        should_monitor = False

        # Wait for monitoring threads to finish
        for hostname, thread in node_threads.items():
            logger.info(f"Waiting for {hostname} monitoring to complete...")
            thread.join(timeout=2)

        # Stop all workloads if requested
        if args.stop_on_exit:
            stop_all_workloads()

        logger.info("Bye! 👋")


if __name__ == "__main__":
    main()
