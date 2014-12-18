MonSQL - Mongodb-style way for using mysql
====================
A mysql wrapper for easy interaction with MySQL using a mongodb-like interface. It's motivated by the fact that mongodb is so easy to use, even for a complete novince! This library is suitable for people don't know much about SQL syntax, but they can still manipulate mysql database through this very simple mongodb-style interface -- query, insert, update, all very easy to understand.  

Usage:  
db = MonSQL(host, port, username, password, dbname)  

Now assume you have a table called image. It works like this:  

	image_tb = db.get('image')
	img_cnt  = image_tb.count()

**select**:
	
	result = image_tb.find({"id": 1}, fields=('id', 'size'))  
	result_count = len(result) 

	for row in result:
		print row.id, row.size, ...

**Complex query operators** Complex queries can be formed using complex operators:

	{a: 1}                              -> a == 1
    {a: {$gt: 1}}                       -> a > 1
    {a: {$gte: 1}}                      -> a >= 1
    {a: {$lt: 1}}                       -> a < 1
    {a: {$lte: 1}}                      -> a <= 1
    {a: {$eq: 1}}                       -> a == 1
    {a: {$in: [1, 2]}}                  -> a == 1
    {a: {$contains: '123'}}             -> a like %123%

    {$not: condition}                   -> !(condition)
    {$and: [condition1, condition2]}    -> condition1 and condition2
    {$or: [condition1, condition2]}     -> condition1 or condition2

**insert**

	image_id = image_tb.insert({"name": "xxx"})  

**update**

	update_count = image_tb.update({"name": "xxx"}, {"name": "xxxx"})  

**delete**

	count = image_tb.remove({"id": 2})  


See? It's just that easy.  

**Update:** now support create/drop operations:

	db.create_table('test_table', [('id INT NOT NULL', 'name VARCHAR(50)')])
	db.get('test_table').truncate()
	db.drop_table('test_table')
