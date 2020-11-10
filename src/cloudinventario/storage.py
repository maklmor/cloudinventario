import logging, re
from pprint import pprint
from datetime import datetime, timedelta
from sqlalchemy.pool import NullPool

import sqlalchemy as sa

TABLE_PREFIX = "ci_"

class InventoryStorage:

   def __init__(self, config):
     self.config = config
     self.dsn = config["dsn"]
     self.engine = self.__create()
     self.conn = None
     self.version = 0

   def __del__(self):
     if self.conn:
       self.disconnect()
     self.engine.dispose()

   def __create(self):
     return sa.create_engine(self.dsn, echo=False, poolclass=NullPool)

   def connect(self):
     self.conn = self.engine.connect()
     #self.conn.execution_options(autocommit=True)
     if not self.__check_schema():
       self.__create_schema()
     self.__prepare();
     return True

   def __check_schema(self):
     return False

   def __create_schema(self):
     meta = sa.MetaData()
     self.source_table = sa.Table(TABLE_PREFIX + 'source', meta,
       sa.Column('id', sa.Integer, primary_key=True, autoincrement=True),
       sa.Column('source', sa.String),
       sa.Column('ts', sa.String, default=sa.func.now()),
       sa.Column('version', sa.Integer, default=1),
       sa.Column('entries', sa.Integer),

       sa.UniqueConstraint('source', 'version')
     )

     self.inventory_table = sa.Table(TABLE_PREFIX + 'inventory', meta,
       sa.Column('inventory_id', sa.Integer, primary_key=True, autoincrement=True),
       sa.Column('version', sa.Integer),

       sa.Column('source', sa.String),
       sa.Column('type', sa.String),
       sa.Column('name', sa.String),
       sa.Column('cluster', sa.String),
       sa.Column('project', sa.String),
       sa.Column('location', sa.String),
       sa.Column('id', sa.String),
       sa.Column('created', sa.String),

       sa.Column('cpus', sa.Integer),
       sa.Column('memory', sa.Integer),
       sa.Column('disks', sa.Integer),
       sa.Column('storage', sa.Integer),

       sa.Column('primary_ip', sa.String),

       sa.Column('os', sa.String),
       sa.Column('os_family', sa.String),

       sa.Column('status', sa.String),
       sa.Column('is_on', sa.Integer),

       sa.Column('owner', sa.String),
       sa.Column('tags', sa.String),

       sa.Column('networks', sa.String),
       sa.Column('storages', sa.String),

       sa.Column('description', sa.String),
       sa.Column('attributes', sa.Text),
       sa.Column('details', sa.Text),

       sa.UniqueConstraint('version', 'source', 'type', 'name', "cluster", 'project', 'id')
     )

     meta.create_all(self.engine, checkfirst = True)
     return True

   def __prepare(self):
     pass

   def __get_source_version_max(self):
     # get active version
     res = self.conn.execute(sa.select([
                   self.source_table.c.source,
                   sa.func.max(self.source_table.c.version).label("version")])
     	        .group_by(self.source_table.c.source))
     res = res.fetchall()
     if res and res[0]["version"]:
       sources = [dict(row) for row in res]
     else:
       sources = []
     return sources

   def save(self, data):
     sources = self.__get_source_version_max()

     # increment versions
     versions = {}
     for source in sources:
       source["version"] += 1
       versions[source["source"]] = source["version"]

     # collect data sources versions
     entries = {}
     for rec in data:
       if rec["source"] not in versions.keys():
         versions[rec["source"]] = 1
         sources.append({ "source": rec["source"],
                          "version": versions[rec["source"]] })
       rec["version"] = versions.get(rec["source"], 1)
       entries.setdefault(rec["source"], 0)
       entries[rec["source"]] += 1

     # save entry counts
     sources_save = []
     for source in sources:
       if not source["source"] in entries:
         continue
       source["entries"] = entries[source["source"]]
       sources_save.append(source)

     if len(sources) == 0:
       return False

     # store data
     with self.engine.begin() as conn:
       conn.execute(self.source_table.insert(), sources_save)
       conn.execute(self.inventory_table.insert(), data)
     return True

   def cleanup(self, days):
     res = self.conn.execute(sa.select([
                   self.source_table.c.source,
                   self.source_table.c.version])
		.where(self.source_table.c.ts <= datetime.today() - timedelta(days=days)))
     res = res.fetchall()

     with self.engine.begin() as conn:
       for row in res:
         logging.debug("prune: source={}, version={}".format(row["source"], row["version"]))
         conn.execute(self.inventory_table.delete().where(
               (self.inventory_table.c.source == row["source"]) &
                  (self.inventory_table.c.version == row["version"])
           ))
         conn.execute(self.source_table.delete().where(
               (self.source_table.c.source == row["source"]) &
                  (self.source_table.c.version == row["version"])
           ))
     return True

   def disconnect(self):
     self.conn.invalidate()
     self.conn.close()
     self.conn = None
     return True
