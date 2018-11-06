import asyncio
import random
import sys

import aiomysql
import logging

logging.basicConfig(level=logging.INFO)
def log(sql, args=()):
	logging.info('SQL:%s' % sql)

@asyncio.coroutine
def create_pool(loop, **kw):
	logging.info('create database connection pool...')
	global __pool
	__pool = yield from aiomysql.create_pool(
		host=kw.get('host', '127.0.0.1'),
		port=kw.get('port', 3306),
		user=kw['user'],
		password=kw['password'],
		db=kw['db'],
		charset=kw.get('charset', 'utf8'),
		autocommit=kw.get('autocommit', True),
		maxsize=kw.get('maxsize', 10),
		minsize=kw.get('minsize', 1),
		loop=loop
		)
	print('create pool done')

async def destroy_pool():
	global __pool
	if __pool is not None:
		__pool.close()
		await __pool.wait_closed()

@asyncio.coroutine
def select(sql, args, size=None):
	log(sql, args)
	global __pool
	with (yield from __pool) as conn:
		cur = yield from conn.cursor(aiomysql.DictCursor)
		yield from cur.execute(sql.replace('?', '%s'), args or ())
		if size:
			rs = yield from cur.fetchmany(size)
		else:
			rs = yield from cur.fetchall()
		yield from cur.close()
		logging.info('rows returned: %s' % len(rs))
		return rs

@asyncio.coroutine
def execute(sql, args):
	log(sql)
	global __pool
	with (yield from __pool) as conn:
		try:
			# excute操作只返回行数，不需要返回dict
			cur = yield from conn.cursor()
			yield from cur.execute(sql.replace('?', '%s'), args)
			yield from conn.commit()
			affected = cur.rowcount
			yield from cur.close()
			print('execute', affected)
		except BaseException as e:
			raise 
		return affected

def create_args_string(num):
	L = []
	for i in range(num):
		L.append('?')
	return (','.join(L))

# ORM映射的基类Model:

class ModelMetaclass(type):

	def __new__(cls, name, bases, attrs):
		'''
		cls 代表要__init__的类
		bases:代表继承父类的集合
		attrs:类的方法集合
		'''

		if name == 'Model':
			return type.__new__(cls, name, bases, attrs)
		tableName = attrs.get('__table__', None) or name
		logging.info('found model: %s (table: %s)' % (name, tableName))
		# 获取Field和主键名:
		mappings = dict()
		fields = [] # 保存非主键的属性名
		primaryKey = None
		for k, v in attrs.items():
			if isinstance(v, Field):
				logging.info('  found mapping: %s ==> %s' % (k,v))
				mappings[k] = v
				if v.primary_key:
					# 找到主键：
					if primaryKey:
						raise RuntimeError('Duplicate primary key for field: %s' % k)
					primaryKey = k
					logging.info('  found primary_key: %s ' % (k))
				else:
					fields.append(k)

		if not primaryKey:
			raise RuntimeError('Primary key not found.')

		# 从类属性中删除该Field属性，否则，容易造成运行时错误（实例的属性会遮盖类的同名属性）
		for k in mappings.keys():
			attrs.pop(k)

		escaped_fields = list(map(lambda f: '`%s`' % f, fields))
		# print('escaped_fields', escaped_fields)
		attrs['__mappings__'] = mappings # 保存属性和列的映射关系
		attrs['__table__'] = tableName
		attrs['__primary_key__'] = primaryKey # 主键属性名
		attrs['__fields__'] = fields # 除主键外的属性名

		attrs['__select__'] = 'select `%s`, %s from `%s`' % (primaryKey, ','.join(escaped_fields), tableName)
		attrs['__insert__'] = 'insert into `%s` (%s, `%s`) values (%s)' % (tableName, ', '.join(escaped_fields), primaryKey, create_args_string(len(escaped_fields) + 1))
		attrs['__update__'] = 'update `%s` set %s where `%s` = ?' %(tableName,','.join(map(lambda f:'`%s` = ?' % (mappings.get(f).name or f),fields)), primaryKey)
		attrs['__delete__'] = 'delete from `%s` where `%s` = ?' %(tableName, primaryKey)
		return type.__new__(cls, name, bases, attrs)

class Model(dict, metaclass=ModelMetaclass):

	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self, key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError(r"'Model' object has no attribute '%s'" % key)

	def __setattr__(self, key, value):
		self[key] = value

	def getValue(self, key):
		return getattr(self, key, None)

	def getValueOrDefault(self, key):
		value = getattr(self, key, None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.debug('using default value for %s: %s' % (key, str(value)))
				setattr(self, key, value)
		return value

class Field(object):

	def __init__(self, name, column_type, primary_key, default):
		self.name = name 
		self.column_type = column_type
		self.primary_key = primary_key
		self.default = default

	def __str__(self):
		return '<%s, %s:%s>' %(self.__class__.__name__, self.column_type, self.name)

class StringField(Field):

	def __init__(self, name=None, primary_key=False, default=None, ddl='varchar(100)'):
		super().__init__(name, ddl, primary_key, default)

class BooleanField(Field):

	def __init__(self, name=None, default=None):
		super().__init__(name, 'Boolean',False, default)

class IntegerField(Field):

	def __init__(self, name=None, primary_key=False, default=0):
		super().__init__(name, 'int', primary_key, default)

class FloatField(Field):

	def __init__(self, name=None, primary_key=False, default=0.0):
		super().__init__(name, 'real',primary_key, default)

class TextField(Field):

	def __init__(self, name=None, default=None):
		super().__init__(name, 'text', False, default)

'''
定义ORM所有映射的基类：Model
Model类可以看做是对所有数据库表操作的基本定义的映射
从dict继承，实现特殊方法__getattr__和__setattr__，能够实现属性操作
'''
class Model(dict, metaclass=ModelMetaclass):
	def __init__(self, **kw):
		super(Model, self).__init__(**kw)

	def __getattr__(self,key):
		try:
			return self[key]
		except KeyError:
			raise AttributeError("'model' object has no attribution:%s"%key)

	def getValue(self,key):
		return getattr(self, key, None)


	def getValueOrDefault(self,key):
		value=getattr(self,key,None)
		if value is None:
			field = self.__mappings__[key]
			if field.default is not None:
				value = field.default() if callable(field.default) else field.default
				logging.info('using default value for %s : %s'%(key,str(value)))
				setattr(self,key,value)
		return value

	@classmethod
	# 申明是类方法：有类变量cls传入，cls可以做一些相关的处理
	# 有子类继承时，调用该方法，传入类变量cls是子类，而非父类
	@asyncio.coroutine
	def find_all(cls, where=None, args=None, **kw):
		sql = [cls.__select__]
		if where:
			sql.append('where')
			sql.append(where)
		if args is None:
			args = []
		orderBy = kw.get('orderBy', None)
		if orderBy:
			sql.append('order by')
			sql.append(orderBy)
		limit = kw.get('limit', None)
		if limit is not None:
			sql.append('limit')
			if isinstance(limit, int):
				sql.append('?')
				args.append(limit)
			elif isinstance(limit, tuple) and len(limit) ==2:
				sql.append('? ?')
				args.append(limit)
			else:
				raise ValueError('invalid limit value:%s' % str(limit))
		# 返回的rs是一个元素是tuple的list
		rs = yield from select(' '.join(sql), args)
		return [cls(**r) for r in rs]


	@classmethod
	@asyncio.coroutine
	def findNumber(cls, selectField, where=None, args=None):
		'''find number by select and where'''
		sql = ['select %s __num__ from `%s`' % (selectField, cls.__table__)]
		if where:
			sql.append('where')
			args.append(where)
		rs = yield from select(' '.join(sql),args,1)
		return rs[0]['__num__']

	@classmethod
	@asyncio.coroutine
	def find(cls,primaryKey):
		'''find object by primary key'''
		rs = yield from select('%s where `%s` = ?'%(cls.__select__,cls.__primary_key__),[primaryKey],1)
		if len(rs) == 0:
			return None
		return cls(**rs[0])

	@classmethod
	@asyncio.coroutine
	def findAll(cls,**kw):
		rs = []
		if len(kw) == 0:
			rs = yield from select(cls.__select__,None)
		else:
			args = []
			values = []
			for k,v in kw.items():
				args.append('%s = ?' %k)
				values.append(v)
			print('%s where %s' % (cls.__select__,' and '.join(args)),values)
			rs = yield from select('%s where %s' % (cls.__select__,' and '.join(args)),values)
		return rs

	@asyncio.coroutine
	def save(self):
		args = list(map(self.getValueOrDefault,self.__fields__))
		args.append(self.getValueOrDefault(self.__primary_key__))
		rows = yield from execute(self.__insert__,args)
		if rows != 1:
			logging.info('failed to inser record:affected rows: %s' % rows)

	@asyncio.coroutine
	def update(self):
		args = list(map(self.getValue,self.__fields__))
		args.append(self.getValue(self.__primary_key__))
		rows = yield from execute(self.__update__,args)
		if rows != 1:
			logging.info('failed to update recod:affected rows:%rows')

	@asyncio.coroutine
	def delete(self):
		args = [self.getValue(self.__primary_key__)]
		rows = yield from execute(self.__delete__, args)
		if rows != 1:
			logging.info('failed tot delete by primary key:affected rows:%s'%rows)

# if __name__ == '__main__':

	# class User(Model):
	# 	id = IntegerField('id', primary_key=True)
	# 	name = StringField('name')
	# 	email = StringField('email')
	# 	password = StringField('password')
    #
	# loop = asyncio.get_event_loop()
    #
	# @asyncio.coroutine
	# def test():
	# 	yield from create_pool(loop=loop, host='127.0.0.1',port=3306,user='root', password='nphb0663',db='test')
	# 	user = User(id=random.randint(5,100),name='cjh', email='cjh@python.com', password='123456')
	# 	yield from user.save()
	# 	print('user:',user)
	# 	r=yield from User.findAll(name='cjh')
	# 	print('r:',r)
	# 	user1 = User(id=2, name='xiong',email='cjh@qq.com',password='123456')
	# 	u = yield from user1.update()
	# 	print('user1:',user1)
	# 	# d = yield from user.delete()
	# 	# print(d)
	# 	s = yield from User.find(2)
	# 	print('s:',s)
	# 	yield from destroy_pool()
    #
	# loop.run_until_complete(test())
	# loop.close()
	# if loop.is_closed():
	# 	sys.exit(0)












