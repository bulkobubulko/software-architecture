import hazelcast

client = hazelcast.HazelcastClient(cluster_name="dev")

hz_map = client.get_map("distributed-map").blocking()

for i in range(1000):
    hz_map.put(i, i)

client.shutdown()
