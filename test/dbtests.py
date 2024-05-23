
# nse.dbConn.execute("ALTER TABLE FnO_Bhavcopy DROP COLUMN new_XPIR;")
# nse.dbConn.execute("ALTER TABLE FnO_Bhavcopy ADD COLUMN new_XPIR TEXT;")
# nse.dbConn.execute("UPDATE FnO_Bhavcopy SET new_XPIR = strftime('%Y-%m-%d', XpryDt, 'unixepoch');")
# nse.dbConn.execute("ALTER TABLE FnO_Bhavcopy DROP COLUMN XpryDt;")
# nse.dbConn.execute("ALTER TABLE FnO_Bhavcopy RENAME COLUMN new_XPIR to XpryDt;")
