from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
import yaml

with open('config.yaml') as f:
    config = yaml.load(f, Loader=yaml.FullLoader)

config = config['cassandra']

auth_provider = PlainTextAuthProvider(
    username=config['username'],
    password=config['password']
)

cluster = Cluster(config['hosts'],
                  auth_provider=auth_provider)

try:
    session = cluster.connect('ks_st50478')
except Exception as e:
    print(e)
    exit(-1)


repond_table = session.execute("Select * from zad3 limit 10")
for row in repond_table:
    print(f'{row[0]}')
