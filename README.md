python-mysql-wrapper
====================
A mysql wrapper for easy interaction with MySQL using a mongodb-like interface. It's motivated by the fact that mongodb is so easy to use, even for a complete novince! This library is suitable for people don't know much about SQL syntax, but they can still manipulate mysql database through this very simple mongodb-style interface -- query, insert, update, all very easy to understand.  

Usage:  
db = MonSQL(db=a MySQLdb db instance)  
Now assume you have a table called image. It works like this:  

**select**: result 			= db.image.find({"id": 1})  
**insert**: image_id 		= db.image.insert({"name": "xxx"})  
**update**: update_count    = db.image.update({"name": "xxx"}, {"name": "xxxx"})  
**delete**: count 			= db.image.remove({"id": 2})  

See? It's just that easy.   
The only differenece with normal mongodb usage is, you need to explicitly call db.commit() to make the change permanent. I believe this is actually better because it allow you to control transaction.   