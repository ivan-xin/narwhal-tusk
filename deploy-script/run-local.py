import subprocess
from os.path import basename, splitext, dirname, join
from time import sleep
from collections import OrderedDict
import json

class NarwhalDeployer:
    BASE_PORT = 9000

    def __init__(self, num_nodes=4, num_workers=1):
        self.num_nodes = num_nodes
        self.num_workers = num_workers
        self.narwhal_root = dirname(dirname(__file__))  # Adjust this path based on script location
        self.node_dir = join(self.narwhal_root, "node")
        self.target_dir = join(self.narwhal_root, "target")
    
    def _create_node_parameters(self):
        params = {
            'header_size': 1_000,
            'max_header_delay': 100,
            'gc_depth': 50,
            'sync_retry_delay': 10_000,
            'sync_retry_nodes': 3,
            'batch_size': 500_000,
            'max_batch_delay': 100
        }
        with open('parameters.json', 'w') as f:
            json.dump(params, f, indent=4)

    def _create_committee(self, keys):
        addresses = OrderedDict()
        for key in keys:
            with open(key, 'r') as f:
                data = json.load(f)
                name = data['name']
                addresses[name] = ['127.0.0.1'] * (1 + self.num_workers)

        committee = {
            'authorities': OrderedDict()
        }
        
        port = self.BASE_PORT
        for name, hosts in addresses.items():
            host = hosts.pop(0)
            primary_addr = {
                'primary_to_primary': f'{host}:{port}',
                'worker_to_primary': f'{host}:{port + 1}'
            }
            port += 2

            workers_addr = OrderedDict()
            for j, host in enumerate(hosts):
                workers_addr[j] = {
                    'primary_to_worker': f'{host}:{port}',
                    'transactions': f'{host}:{port + 1}',
                    'worker_to_worker': f'{host}:{port + 2}',
                }
                port += 3

            committee['authorities'][name] = {
                'stake': 1,
                'primary': primary_addr,
                'workers': workers_addr
            }

        with open('committee.json', 'w') as f:
            json.dump(committee, f, indent=4)

    def _background_run(self, command, log_file):
        # name = splitext(basename(log_file))[0]
        # cmd = f"{command} 2> {log_file}"
        # subprocess.run(["tmux", "new", "-d", "-s", name, cmd], check=True)
        cmd = f"nohup {command} 2> {log_file} &"
        subprocess.run(cmd, shell=True)

    def _kill_nodes(self):
        # Kill all node processes
        subprocess.run("pkill -f node", shell=True, stderr=subprocess.DEVNULL)
        # Kill any remaining nohup processes
        subprocess.run("pkill -f nohup", shell=True, stderr=subprocess.DEVNULL)
        # Give processes time to shut down
    sleep(0.5)


    def deploy(self):
        print("Starting Narwhal deployment...")

        # Kill any previous testbed
        self._kill_nodes()
        subprocess.run("rm -rf logs store keys committee.json parameters.json", shell=True)
        
        # Clean up and compile
        subprocess.run("rm -rf logs ; rm -rf store", shell=True)
        subprocess.run(["cargo", "build", "--release"], cwd=self.narwhal_root)

        # Create necessary directories
        subprocess.run(["mkdir", "-p", "logs", "store", "keys"], check=True)
                # Generate keys and committee config
        node_binary = join(self.target_dir, "release", "node")

        # Create binary aliases
        # subprocess.run("ln -f ../target/release/node node", shell=True)

        # Generate keys and committee config
        keys = []
        for i in range(self.num_nodes):
            key_file = f"keys/node-{i}.json"
            subprocess.run([node_binary, "generate_keys", "--filename", key_file])
            keys.append(key_file)
        
        

        # Generate committee.json with node addresses
        self._create_node_parameters()
        self._create_committee(keys)
        
        # Start primaries
        for i in range(self.num_nodes):
            cmd = f"{join(self.target_dir, 'release', 'node')} run \
                   --keys {keys[i]} \
                   --committee committee.json \
                   --store store/node-{i} \
                   --parameters parameters.json \
                   primary"
            self._background_run(cmd, f"logs/primary-{i}.log")

        # Start workers 
        for i in range(self.num_nodes):
            for j in range(self.num_workers):
                cmd = f"{join(self.target_dir, 'release', 'node')} run \
                    --keys {keys[i]} \
                    --committee committee.json \
                    --store store/worker-{i}-{j} \
                    --parameters parameters.json \
                    worker --id {j}"
                self._background_run(cmd, f"logs/worker-{i}-{j}.log")

        print("Narwhal deployment completed")

if __name__ == "__main__":
    deployer = NarwhalDeployer()
    deployer.deploy()