MonSQL - Light-weighted, MongoDB-style Wrapper for multiple Relational Databases
====================
A light-weighted wrapper for easy interaction with Relational Databases using a mongodb-like interface. It's goal is to be easy to use, even for a complete novince! What's more, it supports seamless switching between different relational databases(currently supported: MySQL, SQLite3) 

More documentation can be found [here](http://monsql.readthedocs.org/en/latest/)

Usage:  

	db = monsql.MonSQL(host, port, username, password, dbname, monsql.DB_TYPES.MYSQL)  

Now assume you have a table called image. It works like this:  

	image_tb = db.get('image')
	img_cnt  = image_tb.count() # total number of rows
	img_cnt  = image_tb.count(distinct=True) # total number of distinct rows

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
    {$and: [condition1, condition2, ...]}    -> condition1 and condition2
    {$or: [condition1, condition2, ...]}     -> condition1 or condition2

**insert**

	image_id = image_tb.insert({"name": "xxx"})  

**update**

	update_count = image_tb.update({"name": "xxx"}, {"name": "xxxx"})  

**delete**

	removed_count = image_tb.remove({"id": 2})  


See? It's just that easy.  

**Update:** now support create/drop operations:

	db.create_table('test_table', [('id INT NOT NULL', 'name VARCHAR(50)')])
	db.truncate_table('test_table')
	db.drop_table('test_table')

### Contribites:

#### TODO:

Pending functionalities include but not limited to:

1.  Add <code>using</code> parameter to <code>Table.remove</code>. This parameter corresponds to the USING clause in <code>DELETE FROM A USING ... WHERE ...</code>

More info in the issue page

#### How to run tests locally

The tests scripts in based on tests/config.yml file. Take a look at the tests/config.yml.orig and modify it to suite your need.


## license:

__The MIT License (MIT)__

Copyright (c) 2014-2015 Taiyuan Zhang <zhangty10@gmail.com>
