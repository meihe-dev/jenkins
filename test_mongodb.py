#!/usr/bin/python3

import pymongo

# 0. 使用 MongoClient 对象，并且指定连接的 URL 地址和要创建的数据库名。
my_client = pymongo.MongoClient("mongodb://localhost:27017/")

'''
创建数据库：my_db = my_client["db1"]
创建集合：teacher_col = my_db["teacher"]
创建文档：
teacher_doc = {"name":"Amy", "gender":"male", "salary":100000}
x = teacher_col.insert_one(teacher_doc)
print(x.inserted_id)

teacher_list = [
    {"name":"Joy1", "gender":"male", "salary":100000},
    {"name":"Joy2", "gender":"male", "salary":100000}
]
x = teacher_col.insert_many(teacher_list)
print(x.inserted_ids)

teacher_list = [
    {"_id":1, "name":"Jon1", "gender":"male", "salary":100000},
    {"_id":2, "name":"Jon2", "gender":"male", "salary":100000}
]
x = teacher_col.insert_many(teacher_list)
'''
# 1. 创建数据库
# 注意: 在 MongoDB 中，数据库只有在内容插入后才会创建! 就是说，数据库创建后要创建集合(数据表)并插入一个文档(记录)，数据库才会真正创建。
my_db = my_client["db1"]

# 2. 判断数据库是否存在
db_list = my_client.list_database_names()
if "db1" in db_list:
    print("数据库已存在！")

# 3. 创建集合
# 集合类似 SQL 的表
# 注意: 在 MongoDB 中，集合只有在内容插入后才会创建! 就是说，创建集合(数据表)后要再插入一个文档(记录)，集合才会真正创建。
teacher_col = my_db["teacher"]

# 4. 判断集合是否已存在
col_list = my_db.list_collection_names()
if "teacher" in col_list:
    print("集合已存在！")

# 5. 插入一个文档到集合中
teacher_doc = {"name":"Amy", "gender":"male", "salary":100000}
# teacher_doc = {"name":"Tim", "gender":"male", "salary":100000}
# teacher_doc = {"name":"Jon", "gender":"male", "salary":100000}
# teacher_doc = {"name":"Joy", "gender":"male", "salary":100000}
x = teacher_col.insert_one(teacher_doc)
print(teacher_doc)  # {'name': 'Amy', 'gender': 'female', 'salary': 10000, '_id': ObjectId('5fc86d8d49b6ebc6c7eb5904')}
print(teacher_col)  # Collection(Database(MongoClient(host=['localhost:27017'], document_class=dict, tz_aware=False, connect=True), 'db1'), 'teacher')
print(x) # <pymongo.results.InsertOneResult object at 0x000001CF03F74E00>

# 6. 返回_id字段
print(x.inserted_id) # 5fc86e893a2449bfc55bd1c8

# 7. 插入多个文档到集合中
teacher_list = [
    {"name":"Joy1", "gender":"male", "salary":100000},
    {"name":"Joy2", "gender":"male", "salary":100000},
    {"name":"Joy3", "gender":"male", "salary":100000},
    {"name":"Joy4", "gender":"male", "salary":100000},
    {"name":"Joy5", "gender":"male", "salary":100000}
]
x = teacher_col.insert_many(teacher_list)
print(x.inserted_ids) # [ObjectId('5fc86effd2aed866c496ae23'), ObjectId('5fc86effd2aed866c496ae24'), ObjectId('5fc86effd2aed866c496ae25'), ObjectId('5fc86effd2aed866c496ae26'), ObjectId('5fc86effd2aed866c496ae27')]

# 8. 插入指定_id的多个文档到集合中
teacher_list = [
    {"_id":1, "name":"Jon1", "gender":"male", "salary":100000},
    {"_id":2, "name":"Jon2", "gender":"male", "salary":100000},
    {"_id":3, "name":"Jon3", "gender":"male", "salary":100000},
    {"_id":4, "name":"Jon4", "gender":"male", "salary":100000},
    {"_id":5, "name":"Jon5", "gender":"male", "salary":100000}
]
x = teacher_col.insert_many(teacher_list)
print(x.inserted_ids) # [1, 2, 3, 4, 5]

'''
查询文档: 将要返回的字段对应值设置为 1
x = teacher_col.find_one()

for x in teacher_col.find():
    print(x)
    
for x in teacher_col.find({}, {"_id": 0, "name": 1, "gender": 1}):
    print(x)
  
除了 _id 你不能在一个对象中同时指定 0 和 1，如果你设置了一个字段为 0，则其他都为 1，反之亦然，以下代码会报错： 
for x in teacher_col.find({},{ "name":1, "gender": 0 }):
  print(x)
    
my_query = {"name": "Jon"}
my_query = {"name": {"$regex": "^J"}}
my_doc = teacher_col.find(my_query)
my_doc = teacher_col.find().limit(3)  
'''
# 9. 查询一条数据
x = teacher_col.find_one()
print(x) #{'_id': ObjectId('5fc86d8d49b6ebc6c7eb5904'), 'name': 'Amy', 'gender': 'female', 'salary': 10000}

# 10. 查询集合中所有数据
# find() 方法可以查询集合中的所有数据，类似 SQL 中的 SELECT * 操作。
# {'_id': ObjectId('5fc86effd2aed866c496ae27'), 'name': 'Joy5', 'gender': 'male', 'salary': 100000}
# {'_id': 1, 'name': 'Jon1', 'gender': 'male', 'salary': 100000}
for x in teacher_col.find():
    print(x)

# 11. 查询指定字段的数据
# 使用 find() 方法来查询指定字段的数据，将要返回的字段对应值设置为 1。
# 以下代码查询返回所有文档的name和对应的gender字段
# {'name': 'Amy', 'gender': 'female'}
# {'name': 'Tim', 'gender': 'male'}
for x in teacher_col.find({}, {"_id": 0, "name": 1, "gender": 1}):
    print(x)

# 除了 _id 你不能在一个对象中同时指定 0 和 1，如果你设置了一个字段为 0，则其他都为 1，反之亦然。
# 以下实例除了 gender 字段外，其他都返回：
for x in teacher_col.find({}, {"gender": 0}):
    print(x)

# 以下代码同时指定了 0 和 1 则会报错：
# pymongo.errors.OperationFailure: Cannot do exclusion on field gender in inclusion projection, full error: {'ok': 0.0, 'errmsg': 'Cannot do exclusion on field gender in inclusion projection', 'code': 31254, 'codeName': 'Location31254'}
# for x in teacher_col.find({},{ "name":1, "gender": 0 }):
#   print(x)

# 12. 根据指定条件查询
# 查找 name 字段为 "Jon" 的数据：
# {'_id': ObjectId('5fc86e55d35bbf103016e823'), 'name': 'Jon', 'gender': 'male', 'salary': 100000}
my_query = {"name": "Jon"}
my_doc = teacher_col.find(my_query)
for x in my_doc:
    print(x)

# 13. 高级查询
# 读取 name 字段中第一个字母 ASCII 值大于 "H" 的数据，大于的修饰符条件为 {"$gt": "H"} :
my_query = {"name": {"$gt": "H"}}
my_doc = teacher_col.find(my_query)
for x in my_doc:
    print(x)

# 14. 使用正则表达式查询
# 正则表达式修饰符只用于搜索字符串的字段。
# 以下实例用于读取 name 字段中第一个字母为 "J" 的数据，正则表达式修饰符条件为 {"$regex": "^J"} :
my_query = {"name": {"$regex": "^J"}}
my_doc = teacher_col.find(my_query)
for x in my_doc:
    print(x)

# 15. 返回指定条数记录
# 使用 limit() 方法，该方法只接受一个数字参数。
my_doc = teacher_col.find().limit(3)
for x in my_doc:
    print(x)

'''
修改数据
'''
# 16. 将name为Joy开头的gender改为female
my_query = {"name": {"$regex": "^Joy"}}
new_values = {"$set": {"gender": "female"}}
# 修改一条
teacher_col.update_one(my_query, new_values)
# 修改所有记录
teacher_col.update_many(my_query, new_values)
for x in teacher_col.find():
    print(x)

'''排序'''
# sort() 方法第一个参数为要排序的字段，第二个字段指定排序规则，1 为升序，-1 为降序，默认为升序。
# 17. 对name字段排序
my_doc = teacher_col.find().sort("name")
my_doc = teacher_col.find().sort("name", -1)
for x in my_doc:
    print(x)

'''删除'''
# 18. 删除一个文档。使用 delete_one() 方法，该方法第一个参数为查询对象，指定要删除哪些数据。
my_query = {"name": "Amy"}
teacher_col.delete_one(my_query)

# 19. 删除多个文档。delete_many() 方法来删除多个文档，该方法第一个参数为查询对象，指定要删除哪些数据。
my_query = {"name": {"$regex": "^Joy"}}
teacher_col.delete_many(my_query)

# 20. 删除集合中的所有文档
x = teacher_col.delete_many({})
print(x.deleted_count, "个文档已删除")

# 21. 删除集合 用 drop() 方法来删除一个集合
teacher_col.drop()