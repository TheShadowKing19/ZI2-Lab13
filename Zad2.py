from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import SimpleStatement
from cassandra import ConsistencyLevel
import yaml
from timeit import default_timer as timer
import pandas as pd
from IPython.display import display

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

consistency_levels = [ConsistencyLevel.ONE, ConsistencyLevel.TWO, ConsistencyLevel.THREE, ConsistencyLevel.QUORUM,
                      ConsistencyLevel.ALL]
consistency_levels_name = ['One', 'Two', 'Three', 'Quorum', 'All']
times = {}

for index, level in enumerate(consistency_levels):
    start = timer()
    query = SimpleStatement("select sum(sumazamowien) from clientview1 where customernumber = 112",
                            consistency_level=level)
    rows = session.execute(query)
    end = timer()
    time = end - start

    times[consistency_levels_name[index]] = time

    print(consistency_levels_name[index])
    for row in rows:
        print(row[0], "\n")

df = pd.DataFrame.from_dict(times, orient='index')
df = df.rename(columns={0: 'Czasy'})
display(df)


