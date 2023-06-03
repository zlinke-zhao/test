import pymssql
from prettytable import PrettyTable
import types
from datetime import datetime
from enum import Enum

# 更新方式的枚举 
class UpdateCondition(Enum):
    Del_and_Insert = 0  # 先删除后插入
    where_Keys = 1      # 按主键更新
    Where_columns = 2   # 按更新列比较更新



gettableinfo_sql = """
    select a.column_id, a.name, convert(int,a.is_identity) as is_identity, convert(int,a.is_computed) as is_computed ,type_name(system_type_id) as columntype, max_length  
    ,(case when a.is_identity=1 OR is_computed = 1 then 'N' else 'Y' end) as updateflag ,  isnull(t.is_pkcolumn,'' ) as is_pkcolumn
    from sys.columns as a 
    outer apply (select 'Y' as is_pkcolumn from sys.index_columns a1 	 
    join sys.indexes as i1 on i1.object_id = a1.object_id and i1.index_id = a1.index_id 
    where a1.object_id = a.object_id  and i1.is_primary_key = 1 
    and a1.object_id = a.object_id and  a1.column_id = a.column_id 	
    ) as t 
    where a.object_id = object_id(%s)
"""
gettableinfo_title =("列ID","列名",'是否自增','是否计算字段','字段类型','长度','可更新','是否主键列')

def ConnectSqlServer(server,database,user,pwd ,autocommit=True):
    conn = pymssql.connect(host=server,user=user,password=pwd,database=database)
    conn.autocommit(autocommit)
    cursor = conn.cursor()
    if not cursor:
        raise Exception('数据库连接失败! server='+server+',user='+user )
        return null
    
    return cursor

def Disconnect(cursor):
    cursor.close()

def messageBox(info,title="提示"):
    print(title,info)
    return

#模仿三元操作符    
def iff(condition,true_data, false_data):    
    return true_data if condition else false_data



# 封装一个数据库事务类
class SqlTrans:
    def __init__(self,server     ,database ,user ,pwd  ,autocommit=True) :
        self._conn = pymssql.connect(host=server,user=user,password=pwd,database=database)
        self._conn.autocommit(autocommit)
        self._cursor = self._conn.cursor()
        if not self._cursor:
            raise Exception('数据库连接失败! server='+server+',user='+user )
    def __del__(self):
        self.Disconnect()
    # 断开连接        
    def Disconnect(self):
        self._cursor.close()
        self._conn.close()
    # 根据表名取得该表各字段信息
    def getTableInfo(self,tablename):        
        self._cursor.execute(gettableinfo_sql,(tablename))
        columninfos  = self._cursor.fetchall()
        if len(columninfos)>0:            
            columninfos.insert(0, gettableinfo_title)
        return columninfos

    # 插入数据保存    
    def insertData(self,datas,table,columns,keys ,updatewherecondition= UpdateCondition.Del_and_Insert):
        if len(datas)<=0:
            return False
        if len(datas[0]) != len(columns):
            messageBox("请确保更新列名个数及其次序，与数据一一相符！",title="提示")
            return False
        checkok, tableinfo = self.checkInsertBefore(table,columns,keys)
        if not checkok:
            return False
        # 生成插入的SQL语句
        sql_list  = []        
        insertsql = "insert into "+table+"("+",".join(columns)+")"
        for i , row in enumerate(datas):
            delsql ="delete a from "+ table+" as a where a."
            chksql ="if not exists (select top 1 1 from "+table+" as a where a."
            for j, key in enumerate(keys):
                index = columns.index(key)
                keysql= ''
                if j ==0:
                    keysql += key+"='"+str(row[index])+"'"
                else:
                    keysql += " and a."+key+"='"+str(row[index])+"'"
                delsql += keysql
                chksql += keysql
            chksql += ")"
            #print(delsql) # 得到删除语句（按主键删除）
            rowsql = "select "
            for j, col in enumerate(columns):                
                data = str(row[j])
                if (data.find("'") >=0):            # 遇到单引号,替换为中文的单引号. 双引号似乎不用更换
                    data = data.replace("'","‘")
                rowsql += iff(j==0,"", " , ")+ iff(data ==None ,  " null " , "'"+(data)+"'" )                 
            rowsql = insertsql +"\n"+rowsql
            #print(rowsql)  # 得到插入的语句
            if updatewherecondition== UpdateCondition.Del_and_Insert:
                sql_list.append(delsql)
                sql_list.append(rowsql)
            else:
                sql_list.append(chksql +"\n" + rowsql)

        # 得到全部的更新语句
        for sqlstr in sql_list:
            #print(s)
            # 执行插入操作. 后续还得研究更新成功行数和失败信息
            try:
                self._cursor.execute(sqlstr)                
            except Exception as e:
                messageBox(sqlstr,"insertData() 执行错误！") 
                print(e)
                pass  
        #self._cursor.commit()
        return True

    # 检测传入的表名、列名、主键(或唯一列、或更新条件列)列等有没有错误(是否存在)
    def checkInsertBefore(self,table,columns,keys):
        tableinfo = self.getTableInfo(tablename=table)
        if len(tableinfo)<=0:
            print("SqlTrans.checkInsertBefore()",'保存异常','未找到需要保存的表'+table+'信息! 请联络开发员检查表名是否传入错误!')
            return False ,None
        cols = [col[1] for col in tableinfo if col[1] in columns ]
        wherecols = [col[1] for col in tableinfo if col[1] in keys ]
        if len(cols) != len(columns):
            print("SqlTrans.checkInsertBefore()",'保存异常','需要保存的列名,不完全存在于表'+table+'中!')
            return False, tableinfo
        if len(wherecols) != len(keys):
            print("SqlTrans.checkInsertBefore()",'保存异常','主键列或条件列,不完全存在于表'+table+'中!')
            return False, tableinfo
        # 返回正常    
        return True, tableinfo
#查询结果数据集
class DataSet :
    #构造函数
    def __init__(self,cursor):
        self.col_name_list = [tuple[0] for tuple in cursor.description]
        self.cols       = self.col_name_list    # 列名清单(list[])
        allrows         = cursor.fetchall()     # 各行数据(元组数据,用小括号的那种,转成list)
        self.rows       = [] 
        for row in allrows:
            self.rows.append(list(row))

        self.primaryKey = []                    # 主键列(list)
        self.tableName  = ""                    # 可更新的表名
        self.cursor     = cursor                # 可用游标
        self.tableInfo  = None                  # 表结构对象(DataSet)
        self.rowcounts   = self.rowcount()
        self.ROWNEW      = 1    # 新行
        self.ROWMODIFIED = 2    # 修改行
        self.ROWDELETED  = -1   # 删除行
        self.NOTMODIFIED = 0    # 未变更
        self.original    = self.rows.copy() # 旧数据
        #加个行标志
        self.rowStatus  = [self.NOTMODIFIED for i in self.rows] # 行状态标志, 0 表示未变更, -1 表示删除, 1 表示新增, 2 表示修改
    # 取得数据表的结构定义
    def getTableInfor(self,tblname=""):
        if tblname == "":
            tblname = self.tableName       

        self.cursor.execute(gettableinfo_sql,(tblname))
        dt = DataSet(self.cursor)
        if tblname == self.tableName:
            self.tableInfo = dt
        return dt

    #根据行号,列号,取值
    def getitem(self,row,colname):
        if type(colname) is int :
            col = colname
        else:
            col = self.col_name_list.index(colname)
        if row>=0 and col >=0 :
            return self.rows[row][col]
        return None
    #设置值
    def setitem(self,row,colname,data):
        col = self.col_name_list.index(colname)
        self.rows[row][col] = data
        if self.rowStatus[row] == self.NOTMODIFIED:
            self.rowStatus[row] = self.ROWMODIFIED 
    #增加新行
    def addRow(self):
        newrow = list()
        for col in self.cols:
            newrow.append(None)
        self.rows.append(newrow)
        rowid =self.rowcount() -1
        self.rowStatus.append( self.ROWNEW )
        return rowid
    #数据行数
    def rowcount(self):
        return len(self.rows)
    def columncount(self):
        return len(self.col_name_list)
    #根据列ID取列名
    def getcolumnname(self,colid):
        return self.col_name_list[colid]
    #列名是否存在
    def columnexists(self,colname):
        col = self.col_name_list.index(colname)
        return col>= 0

    #输出格式
    def output(self):
        x=PrettyTable(self.col_name_list)
        #print(self.col_name_list)
        for row in self.rows:
            x.add_row(row)
        print(x)

    def setSave(self, table, primarykey=""):
        self.tableName = table
        self.getTableInfor()
        if primarykey=="":
            self.primaryKey = []
            for i in range(self.tableInfo.rowcount()):
                if self.tableInfo.getitem(i,"is_pkcolumn")=="Y" :
                    self.primaryKey.append(self.tableInfo.getitem(i,"name"))

            #new2 = [x for x in data if x >= 0]

        else:
            self.primaryKey = primarykey.split(",")
        return
    # 保存. updatewhere=0: 按主键更新; updatewhere=1 按主键+可更新列更新; updatewhere= 2 按主键+更改列更新(这个还没有实现)  
    def saveInsert(self,updatewhere=0):
        if (self.primaryKey == "" or self.tableName ==""):
            messageBox("未设置更新表或主键!无法执行更新操作")
            return False
        sql = "insert into "+ self.tableName+"("  
        enableupdatecols = []
        for i in range(self.tableInfo.rowcount()):
            if(self.tableInfo.getitem(i,'is_identity')== True):
                continue
            enableupdatecols.append(self.tableInfo.getitem(i,'name'))
        sql = sql + ",".join (enableupdatecols)+")"
        #print(len(enableupdatecols),sql)
        #逐行处理
        checksql = "select top 1 1 from "+self.tableName+" "
        insertrowcount = 0
        updaterowcount = 0
        for row in range(self.rowcount()):
            # 1. 如果是未变更行,则跳过
            if self.rowStatus[row] == self.NOTMODIFIED:
                continue

            insertline   = " insert into "+self.tableName +"("
            selectline   = " select "
            updateline   = " update "+self.tableName +" set "
            whereline    = " where "
            updatedata   = []
            wheredata    = []
            insertcols   = []
            insertdata   = []
            # 2.检测主键是否重复
            pkcondition = ""
            for col in self.primaryKey:
                updatedata.append( col+" ='"+str(self.getitem(row,col))+"' " )                
                #pkcondition += (" " if pkcondition=="" else " and " )+ ( col+" ='"+str(self.getitem(row,col))+"' " )
            pkcondition = " and ".join(updatedata)

            # 3. 如果是删除行,则产生删除语句
            if self.rowStatus[row] == self.ROWDELETED :
                sqlstr = "delete a from "+self.tableName +" where " + pkcondition 
            else:
                updatedata   = []
                updaterowcount = 0
                insertrowcount = 0
                for j in range(len(enableupdatecols)):
                    data = self.getitem(row, enableupdatecols[j])
                    col  = enableupdatecols[j]
                    if data == None:
                        continue
                    # 4. 如果是数据变更,则产生更新语句
                    if self.rowStatus[row] == self.ROWMODIFIED:
                        data = self.getitem(row,col)
                        if isinstance(data,bool):
                            data = iff(data,1,0)
                        updatedata.append( col+" ='"+str(data)+"' " )
                        if updatewhere > 0 : # 如果按可更新列来更新,则把这些也加入条件
                            wheredata.append( col+" ='"+str(data)+"' " )                             
                    else:
                        # 5. 新增行,产生新增语句,但也要加上按主键列检测是否值存在
                        #insertline += ("" if j==0 else ",") + col
                        insertcols.append(col)
                        if isinstance(data,str):
                            if (data.find("'") >=0):            # 遇到单引号,替换为中文的单引号. 双引号似乎不用更换
                                data = data.replace("'","‘")
                            insertdata.append(" '" +data+"'")
                            #selectline += ("'" if j==0 else ",'") +data+"'"
                        elif isinstance(data,datetime):
                            insertdata.append(" '" +str(data)+"'")
                            #selectline += ("'" if j==0 else ",'") +str(data)+"'"
                        elif isinstance(data,bool):
                            insertdata.append(str(iff(data,1,0)))
                            #selectline += (""  if j==0 else ",") + str(1 if data else 0)
                        else: # 数字
                            insertdata.append(str(data))
                            #selectline += (""  if j==0 else ",") + str(data)
                
                # 好，现在拼接完整的插入SQL(逐行)
                if len(insertdata)>0:
                    insertrowcount+=1
                    insertline += ",".join(insertcols)
                    selectline += ",".join(insertdata)
                    sqlstr = "if not exists ("+checksql  + whereline + pkcondition +") \n" + insertline + ")\n " +selectline
                if len(updatedata)>0: 
                    updaterowcount +=1
                    updateline = updateline + ",".join(updatedata) + whereline + pkcondition + " and ".join(wheredata) 
            # 执行插入操作.
            try:
                #self.cursor.execute(sqlstr)
                if insertrowcount >0 :
                    messageBox(sqlstr,insertrowcount)
                messageBox("--------------------------------------")
                if updaterowcount > 0 :
                    messageBox(updateline,updaterowcount)
            except:
                pass    
            #self.cursor.executemany(sqlstr)
            #self.cursor.commit()
            #print(sqlstr)


        return True




def test_db():     
    cursor =ConnectSqlServer()
    sql ="select * from crypto_projects"
    sql ="select top 2 * from crypto_proj_investor a where a.zbid=%s"
    sql = "select top 3 * from crypto_projects "
    cursor.execute(sql,("1"))
    dt = DataSet(cursor)
    #dt.output()
    dt.setitem(1,'project_domain','111111111')
    dt.setitem(2,'project_domain','3222222222')
    i = dt.addRow()
    dt.setitem(i,'project_id', "BTC TestCoin")
    dt.setitem(i,'project_link', "www.baidu.com")
    dt.setitem(i,'project_date', "2022-12-20")
    dt.setitem(i,'project_domain', "测试开发")
    
    dt.setSave("crypto_projects", "project_id")
    dt.saveInsert()


    #dt3 =[x for x in dt2["name"] if dt2["is_pkcolumn"] == "Y"]
    #print(dt3)
    Disconnect(cursor)
    #cols = ",".join (dt.col_name_list)
    #print (cols)

def test_db2():
    mysqlca  = SqlTrans()
    cols = mysqlca.getTableInfo("crypto_projects")
    #print(cols)
    rc , tblinfo =mysqlca.checkInsertBefore('crypto_projects',['project_id','project_date','id','project_link'],['project_id'])
    print(rc,tblinfo)
    mysqlca.Disconnect()
def test_db3():
    table = "crypto_projects"
    datas = [('列ID', '列名', '是否自增', '是否计算字段', '字段类型', '长度', '可更新', '是否主键列'), (1, 'project_id', 0, 0, 'varchar', 50, 'Y', ''), (2, 'project_link', 0, 0, 'nvarchar', 200, 'Y', ''), (3, 'project_date', 0, 0, 'datetime', 8, 'Y', ''), (4, 'project_domain', 0, 0, 'nvarchar', 200, 'Y', ''), (5, 'project_amount', 0, 0, 'numeric', 9, 'Y', ''), (6, 'project_companys', 0, 0, 'nvarchar', 2000, 'Y', ''), (7, 'project_info', 0, 0, 'nvarchar', 8000, 'Y', ''), (8, 'financing', 0, 0, 'nvarchar', 20, 'Y', ''), (9, 'iscoin', 0, 0, 'char', 1, 'Y', ''), (10, 'tokenname', 0, 0, 'nvarchar', 20, 'Y', ''), (11, 'defi', 0, 0, 'bit', 1, 'Y', ''), 
(12, 'gamefi', 0, 0, 'bit', 1, 'Y', ''), (13, 'nft', 0, 0, 'bit', 1, 'Y', ''), (14, 'meta', 0, 0, 'bit', 1, 'Y', ''), (15, 'wallet', 0, 0, 'bit', 1, 'Y', ''), (16, 'incubator', 0, 0, 'bit', 1, 'Y', ''), (17, 'social', 0, 0, 'bit', 1, 'Y', ''), (18, 'cex', 0, 0, 'bit', 1, 'Y', ''), (19, 'DAO', 0, 0, 'bit', 1, 'Y', ''), (20, 'Web3', 0, 0, 'bit', 1, 'Y', ''), (21, 'Base', 0, 0, 'bit', 1, 'Y', ''), (22, 'Oth', 0, 0, 'bit', 1, 'Y', ''), (23, 'id', 1, 0, 'int', 4, 'N', 'Y'), (24, 'inputdate', 0, 0, 'datetime', 8, 'Y', ''), (25, 'userid', 0, 0, 'nvarchar', 40, 'Y', ''), (26, 'lastupdate', 0, 0, 'datetime', 8, 'Y', ''), (27, 'lastupdateid', 0, 0, 'nvarchar', 40, 'Y', ''), (28, 'remark', 0, 0, 'nvarchar', 1000, 'Y', ''), (29, 'website', 0, 0, 'nvarchar', 200, 'Y', ''), (30, 'follow', 0, 0, 'char', 1, 'Y', ''), (31, 'website_docs', 0, 0, 'nvarchar', 200, 'Y', ''), (32, 'github', 0, 0, 'nvarchar', 200, 'Y', ''), (33, 'whitepaper', 0, 0, 'nvarchar', 200, 'Y', ''), (34, 'money', 0, 0, 'nvarchar', 40, 'Y', '')]
    columns = ["linkid" ,"project_date", "project_id","project_domain","money","project_companys","project_info","project_link","project_amount"]
    insertsql = "insert into "+table+"("+",".join(columns)+")"
    #print(insertsql)
    print(columns.index("project_id"))
def main():
    test_db3()
    #dt=c.readKline("BTC_USDT","huobi","1day",datetime.strptime('2022-09-01', '%Y-%m-%d'),datetime.strptime('2022-09-20', '%Y-%m-%d'))
    #dt.output()

    return


if __name__ == '__main__':
    main()








