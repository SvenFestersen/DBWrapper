import dbwrapper.db

def run_test():
    db = dbwrapper.db.DBWrapper()
    test_create_table(db)
    test_insert(db)
    test_select(db)
    test_drop_table(db)
    db.close()
    
def test_create_table(db):
    tables = db.get_tables()
    print tables
    
    if not "test" in tables:
        db.execute("CREATE TABLE test (id TEXT, value TEXT)")
        
    tables = db.get_tables()
    print tables
    
def test_insert(db):
    for i in range(0, 1000):
        db.execute("INSERT INTO test VALUES (?,?)", ("key%s" % i, "value%s" % i))
    
def test_select(db):
    result = db.execute("SELECT id, value FROM test")
    for row in result:
        print "%s: %s" % (row["id"], row["value"])
    
def test_drop_table(db):
    tables = db.get_tables()
    print tables
    
    if "test" in tables:
        db.execute("DROP TABLE test")
        
    tables = db.get_tables()
    print tables
    
if __name__ == "__main__":
    run_test()
