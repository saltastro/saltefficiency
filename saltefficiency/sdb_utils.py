
"""Tools and queries for the sdb
"""

def getnightinfo(sdb, obsdate):
    """Get the NightInfo_Id for an observing date

    Parameters
    ----------
    sdb: ~mysql.mysql
       A connection to the sdb database

    obsdate: str
       Observing date in YYYYMMDD format

    """

    return sdb.select('NightInfo_Id', 'NightInfo', 'Date=%s' % obsdate)[0][0]

