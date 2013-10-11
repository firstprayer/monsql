python-mysql-wrapper
====================
This is a mongodb style MySQL Wrapper
Usage:
manager = MySQLManager(db=a MySQLdb db instance, source=source, mode=transaction_mode)

select: result = manager.find({"id": 1})
insert: manager.insert({"name": "xxx"})
update: manager.update({"name": "xxx"}, {"name": "xxxx"})
delete: manager.remove({"id": 2})

Setting source: manager.source(new_source), a source could be a string(table name), or a query object

Notice, in the DEFAULT case, one need to manually call manager.commit() after insert/update/delete