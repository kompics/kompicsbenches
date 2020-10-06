# AWS distributed deployment (Inter-VPC)

1. Create VPC.
2. Create Internet Gateway and connect the VPC created in 1) to it.
3. Create subnet using same CIDR as VPC, enable public IP.
4. Create VPC peer connection request to another VPC (that has been created following the previous steps).
5. Edit routing table: 0.0.0.0 to Internet Gateway and private IP of VPC peer to corresponding connection created in 4).
6. Create/edit security group to allow TCP communication from private IP subnet of VPC peers and SSH from master.
7. Create EC2 instance in the subnet that was created in 3).

Each of the steps above need to be performed on all regions.

8. Edit the `nodes.conf` file at the master with the private IPs of the clients.

## Additional steps that might be needed
- Generate new key pair and use the private key as default on master. Add the generated public key of master in `authorized_keys` of all clients.
- Add the path of the private key to [here](https://github.com/haraldng/kompicsbenches/blob/master/bench.sc#L316) in `bench.sc`. **(TODO: command line argument)**