from __future__ import annotations
import os
from .cluster import Cluster
def discover_nodes(value=None):
    raw=value if value is not None else os.environ.get('AIGURUBOOT_NODES','')
    cluster=Cluster()
    for item in raw.split(','):
        item=item.strip()
        if item: cluster.add(item)
    return cluster.health()
