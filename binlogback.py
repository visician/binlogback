#!/usr/bin/env python
# -*- coding: utf-8 -*-
#author:zhaokeke
#email:zhkeke2008@163.com

import time
import sys,os,struct
import math
import getopt
import gc
VERSION="0.1"


class EventType:
	UNKNOWN_EVENT				=	0x00
	START_EVENT_V3				=	0x01
	QUERY_EVENT					=	0x02
	STOP_EVENT					=	0x03
	ROTATE_EVENT				=	0x04
	INTVAR_EVENT				=	0x05
	LOAD_EVENT					=	0x06
	SLAVE_EVENT					=	0x07
	CREATE_FILE_EVENT			=	0x08
	APPEND_BLOCK_EVENT			=	0x09
	EXEC_LOAD_EVENT				=	0x0a
	DELETE_FILE_EVENT			=	0x0b
	NEW_LOAD_EVENT				=	0x0c
	RAND_EVENT					=	0x0d
	USER_VAR_EVENT				=	0x0e
	FORMAT_DESCRIPTION_EVENT	=	0x0f
	XID_EVENT					=	0x10
	BEGIN_LOAD_QUERY_EVENT		=	0x11
	EXECUTE_LOAD_QUERY_EVENT	=	0x12
	TABLE_MAP_EVENT				=	0x13
	WRITE_ROWS_EVENTv0			=	0x14
	UPDATE_ROWS_EVENTv0			=	0x15
	DELETE_ROWS_EVENTv0			=	0x16
	WRITE_ROWS_EVENTv1			=	0x17
	UPDATE_ROWS_EVENTv1			=	0x18
	DELETE_ROWS_EVENTv1			=	0x19
	INCIDENT_EVENT				=	0x1a
	HEARTBEAT_EVENT				=	0x1b
	IGNORABLE_EVENT				=	0x1c
	ROWS_QUERY_EVENT			=	0x1d
	WRITE_ROWS_EVENTv2			=	0x1e
	UPDATE_ROWS_EVENTv2			=	0x1f
	DELETE_ROWS_EVENTv2			=	0x20
	GTID_EVENT					=	0x21
	ANONYMOUS_GTID_EVENT		=	0x22
	PREVIOUS_GTIDS_EVENT		=	0x23

class StaticVar:
	INT8=256
	INT16=65536
	INT24=16777216
	INT32=4294967296
	
	

	
class MysqlTypeDef:
	MYSQL_TYPE_DECIMAL	=	0x00
	MYSQL_TYPE_TINY	=	0x01
	MYSQL_TYPE_SHORT	=	0x02
	MYSQL_TYPE_LONG	=	0x03
	MYSQL_TYPE_FLOAT	=	0x04
	MYSQL_TYPE_DOUBLE	=	0x05
	MYSQL_TYPE_NULL	=	0x06
	MYSQL_TYPE_TIMESTAMP	=	0x07
	MYSQL_TYPE_LONGLONG	=	0x08
	MYSQL_TYPE_INT24	=	0x09
	MYSQL_TYPE_DATE	=	0x0a
	MYSQL_TYPE_TIME	=	0x0b
	MYSQL_TYPE_DATETIME	=	0x0c
	MYSQL_TYPE_YEAR	=	0x0d
	MYSQL_TYPE_NEWDATE	=	0x0e
	MYSQL_TYPE_VARCHAR	=	0x0f
	MYSQL_TYPE_BIT	=	0x10
	MYSQL_TYPE_TIMESTAMP2	=	0x11
	MYSQL_TYPE_DATETIME2	=	0x12
	MYSQL_TYPE_TIME2	=	0x13
	MYSQL_TYPE_NEWDECIMAL	=	0xf6
	MYSQL_TYPE_ENUM	=	0xf7
	MYSQL_TYPE_SET	=	0xf8
	MYSQL_TYPE_TINY_BLOB	=	0xf9
	MYSQL_TYPE_MEDIUM_BLOB	=	0xfa
	MYSQL_TYPE_LONG_BLOB	=	0xfb
	MYSQL_TYPE_BLOB	=	0xfc
	MYSQL_TYPE_VAR_STRING	=	0xfd
	MYSQL_TYPE_STRING	=	0xfe
	MYSQL_TYPE_GEOMETRY	=	0xff
class Common:
	@staticmethod
	def unpack6ByteInt(stream):
		try:
			return ((struct.unpack('i',stream.read(4)))[0]+(struct.unpack('h',stream.read(2)))[0]*StaticVar.INT32)
		except Exception,e:
			raise e	
	@staticmethod
	def unpack4ByteInt(stream):
		try:
			return (struct.unpack('i',stream.read(4)))[0]	
		except Exception,e:
			raise e	
	@staticmethod
	def unpack2ByteInt(stream):
		try:
			return (struct.unpack('h',stream.read(2)))[0]	
		except Exception,e:
			raise e
	@staticmethod
	def unpack1ByteInt(stream):
		try:
			return (struct.unpack('B',stream.read(1)))[0]	
		except Exception,e:
			raise e		
	@staticmethod
	def unpack8ByteInt(stream):
		try:
			return (struct.unpack('q',stream.read(8)))[0]	
		except Exception,e:
			raise e	
	@staticmethod
	def unpack3ByteInt(stream):
		try:
			return ((struct.unpack('B',stream.read(1)))[0]+(struct.unpack('B',stream.read(1)))[0]*StaticVar.INT8+(struct.unpack('B',stream.read(1)))[0]*StaticVar.INT16)
		except Exception,e:
			raise e		
	@staticmethod
	def unpackFloat(stream):
		try:
			return str((struct.unpack('f',stream.read(4)))[0])
		except Exception,e:
			raise e	
	@staticmethod
	def unpackDouble(stream):
		try:
			return str((struct.unpack('d',stream.read(8)))[0])
		except Exception,e:
			raise e		
	@staticmethod
	def unpackVarString(stream,length):
		try:
			length2=0
			if(length==1):
				length2,=struct.unpack('B',stream.read(1))
			else:
				length2,=struct.unpack('h',stream.read(2))
			return stream.read(length2)	
		except Exception,e:
			raise e
	@staticmethod
	def unpackString(stream,length):
		try:
			length2=0
			if(length==1):
				length2,=struct.unpack('B',stream.read(1))
			else:
				length2,=struct.unpack('h',stream.read(2))
			return stream.read(length2)	
		except Exception,e:
			raise e	
	@staticmethod
	def unpackBlob(stream,length):
		try:
			length2=0
			if(length==1):
				length2,=struct.unpack('B',stream.read(1))
			elif(length==2):
				length2,=struct.unpack('h',stream.read(2))
			elif(length==3):
				length2=Common.unpack3ByteInt(stream)
			else:
				length2,=struct.unpack('i',stream.read(4))	
			return stream.read(length2)	
		except Exception,e:
			raise e	
	@staticmethod
	def unpackDatetime(stream):
		try:
			datetimeLong,=struct.unpack('q',stream.read(8))
			buf1=str(datetimeLong).zfill(14)
			buf2=tuple(buf1)
			year=''.join(buf2[0:4])
			month=''.join(buf2[4:6])
			day=''.join(buf2[6:8])
			hour=''.join(buf2[8:10])
			minute=''.join(buf2[10:12])
			second=''.join(buf2[12:14])
			return (year.zfill(4)+'-'+month.zfill(2)+'-'+day.zfill(2)+' '+hour.zfill(2)+':'+minute.zfill(2)+':'+second.zfill(2))
		except Exception,e:
			raise e			
	@staticmethod
	def unpackDate(stream):
		try:
			buf1=Common.unpack3ByteInt(stream)
			year=str(buf1/(16*32))
			day=str(buf1%32)
			month=str((buf1-int(day)-int(year)*16*32)/32)
			return (year.zfill(4)+'-'+month.zfill(2)+'-'+day.zfill(2))
		except Exception,e:
			raise e
	# if column value not in （00:00:00-838:59:59）,the value will be invalid ,also mysqlbinlog 5.5 5.6 will tell a invalid value
	@staticmethod
	def unpackTime(stream):
		try:
			buf1=Common.unpack3ByteInt(stream)
			hour=str(buf1/10000)
			minute=str((buf1-int(hour)*10000)/100)
			second=str(buf1-int(hour)*10000-int(minute)*100)
			return (hour.zfill(2)+':'+minute.zfill(2)+':'+second.zfill(2))
		except Exception,e:
			raise e			
	@staticmethod
	def unpackTimestamp(stream):
		try:
			stamp,=struct.unpack('i',stream.read(4))
			nowTime=time.localtime(stamp)
			return time.strftime('%Y-%m-%d %H:%M:%S',nowTime)
		except Exception,e:
			raise e
	@staticmethod	
	def unpackLenencInt(stream):
		flag,=struct.unpack('B',stream.read(1))
		try:
			#<0xfb 1 byte flag
			if(flag<0xfb):
				return flag
			#0xfc followed by 2 byte int
			if(flag==0xfc):
				return (struct.unpack('h',stream.read(2)))[0]
			#0xfd followed by 3 byte int	
			if(flag==0xfd):
				return (struct.unpack('h',stream.read(2)))[0] +(struct.unpack('B',stream.read(1)))[0]*StaticVar.INT16	
			#0xfe followed by 8 byte int
			if(flag==0xfe):
				return (struct.unpack('q',stream.read(8)))[0]
		except Exception,e:
			raise e
	@staticmethod			
	def unpackBit(stream,byteLength):
		try:
			bitToInt=int(stream.read(byteLength).encode('hex'),16)
			return bitToInt
		except Exception,e:
			raise e		
			
			
	#return column meta-def and column type of each column 
	@staticmethod
	def metaDefOfMysqlType(mysqlTypeArr,stream):
		result=[]
		try:
			for mysqlType in mysqlTypeArr:
				#decimal  (mysqlType,(pricision,frac))
				if(mysqlType==MysqlTypeDef.MYSQL_TYPE_DECIMAL or  mysqlType== MysqlTypeDef.MYSQL_TYPE_NEWDECIMAL):
					buf1,=struct.unpack('B',stream.read(1))
					buf2,=struct.unpack('B',stream.read(1))
					result.append((mysqlType,(buf1,buf2)))
					continue
				#varchar or blob,return column type and the length of byte to store string size
				elif(mysqlType==MysqlTypeDef.MYSQL_TYPE_VARCHAR or mysqlType==MysqlTypeDef.MYSQL_TYPE_VAR_STRING):
					varcharMetaLength,=struct.unpack('h',stream.read(2))
					varcharLengthBytes=1
					if(varcharMetaLength<=255):
						varcharLengthBytes=1
					else:
						varcharLengthBytes=2
					result.append((mysqlType,varcharLengthBytes))
					continue
				#char(M) binary(M) max(M)=255,binary(n) means the length of bytes is n,char(n) means the length of string is n.
				#if M=100 charset=utf8,need 300 bytes to store, 300/256=1, so first byte is (1^0xf)*16 +0xe,second byte is 300%256
				elif(mysqlType==MysqlTypeDef.MYSQL_TYPE_STRING):
					buf1,=struct.unpack('B',stream.read(1))
					buf2,=struct.unpack('B',stream.read(1))
					varcharLengthBytes=1
					#for type of set and enum,
					if(buf1 in (248,247)):
						varcharLengthBytes=buf2
						result.append((mysqlType,(buf1,varcharLengthBytes)))
						continue
					
					if(buf1==254):
						varcharLengthBytes=1
					else:
						varcharLengthBytes=2
					result.append((mysqlType,(buf1,varcharLengthBytes)))
					continue
				#bit(n),(n+7)/8 bytes to store 
				elif(mysqlType==MysqlTypeDef.MYSQL_TYPE_BIT):
					buf1,=struct.unpack('B',stream.read(1))
					buf2,=struct.unpack('B',stream.read(1))
					metaDefLength=buf1+buf2*8
					bitByteLength=(metaDefLength+7)/8
					result.append((mysqlType,bitByteLength))
					continue
				#float or double,return column type and size of byte to store (float|double)
				elif(mysqlType==MysqlTypeDef.MYSQL_TYPE_BLOB or mysqlType==MysqlTypeDef.MYSQL_TYPE_DOUBLE or mysqlType==MysqlTypeDef.MYSQL_TYPE_FLOAT):
					byteLength,=struct.unpack('B',stream.read(1))
					result.append((mysqlType,byteLength))
					continue
				elif(mysqlType==MysqlTypeDef.MYSQL_TYPE_BIT):
					byteLength=(struct.unpack('B',stream.read(1)))[0]+(struct.unpack('B',stream.read(1)))[0] * 8
					byteLength=(byteLength+7)/8
					result.append((mysqlType,byteLength))
					continue
				else:
					result.append((mysqlType,0))
					continue
			return result
		except Exception,e:
			raise e	
	#decimal(15,3) precision=15 frac=3 intg=15-3
	#intg=12, devide 12 to 3 digits and 9 digits
	#3 digits stores in 2 bytes int, 9 digits stored in 4 bytes int,frac 3 digits stores in 2 bytes int
	#for example +123456789012.123
	# 123  456789012  123 
	# 007b 1b3a0c14   007b
	# because "+" ,reverse first bit
	# 807b
	@staticmethod
	def unpackDecimal(stream,precision,frac):	
		intg=precision-frac
		#array to stores intg bytes
		intgArray=[]
		intg9=intg/9
		intgLeft=intg%9
		if(intgLeft in (1,2)):
			intgArray.append(1)
		elif (intgLeft in (3,4)):
			intgArray.append(2)
		elif (intgLeft in (5,6)):
			intgArray.append(3)
		elif (intgLeft in (7,9)):
			intgArray.append(4)
		for i in range(0,intg9):
			intgArray.append(4)		
		fracArray=[]
		frac9=frac/9
		fracRight=frac%9
		flag=0
		while(flag<frac9):
			fracArray.append(4)		
			flag=flag+1
		if(fracRight in (1,2)):
			fracArray.append(1)
		elif (fracRight in (3,4)):
			fracArray.append(2)
		elif (fracRight in (5,6)):
			fracArray.append(3)
		elif (fracRight in (7,9)):
			fracArray.append(4)
		else:
			pass
		#read first byte to determinate >0 or <0
		firstFlag,=struct.unpack('B',stream.read(1))
		stream.seek(stream.tell()-1)
		#1  means >0  -1 means <0
		plusMinus=0
		#leftStr
		leftStr=''
		#rightStr
		rightStr=''
		if(firstFlag>127):
			plusMinus=1
		elif(firstFlag<127):
			plusMinus=-1
		else:
			raise Exception("error occur when unpack decimal type,error msg:%s"%(e))
		#  decimal>0
		if(plusMinus==1):
			if(len(intgArray)==0):
				leftStr='0'
			else:
				try:
					for i in range(0,len(intgArray)):
						byteLength=intgArray[i]
						fillLength=0
						buf1=0
						buf2=0
						if(i==0):
							fillLength=intgLeft
							buf1=int(stream.read(byteLength).encode('hex'),16)
							if(byteLength==1):
								buf2=buf1 ^ 0x80
							elif(byteLength==2):
								buf2=buf1 ^ 0x8000
							elif(byteLength==3):
								buf2=buf1 ^ 0x800000
							elif(byteLength==4):
								buf2=buf1 ^ 0x80000000
						else:
							fillLength=9
							buf2=int(stream.read(byteLength).encode('hex'),16)
						leftStr=leftStr+str(buf2).zfill(fillLength)							
				except Exception,e:
					raise Exception("error occur when unpack decimal type,error msg:%s"%(e))
			if(len(fracArray)==0):
				rightStr='0'
			else:
				try:
					for i in range(0,len(fracArray)):
						byteLength=fracArray[i]
						fillLength=0
						buf1=0
						buf2=0
						if(i==0 and len(intgArray)==0):
							if((len(fracArray)-1)!=i):
								fillLength=9
							else:
								fillLength=fracRight
							buf1=int(stream.read(byteLength).encode('hex'),16)
							if(byteLength==1):
								buf2=buf1 ^ 0x80
							elif(byteLength==2):
								buf2=buf1 ^ 0x8000
							elif(byteLength==3):
								buf2=buf1 ^ 0x800000
							elif(byteLength==4):
								buf2=buf1 ^ 0x80000000
						else:
							if((len(fracArray)-1)!=i):
								fillLength=9
							else:
								fillLength=fracRight
							buf2=int(stream.read(byteLength).encode('hex'),16)
						rightStr=rightStr+str(buf2).zfill(fillLength)
				except Exception,e:
					raise Exception("error occur when unpack decimal type,error msg:%s"%(e))
			return (leftStr+'.'+rightStr)
		#decimal<0
		else:
			if(len(intgArray)==0):
				leftStr='0'
			else:
				try:
					for i in range(0,len(intgArray)):
						byteLength=intgArray[i]
						fillLength=0
						buf1=0
						buf2=0
						if(i==0):
							fillLength=intgLeft
							buf1=int(stream.read(byteLength).encode('hex'),16)
							if(byteLength==1):
								buf2=buf1 ^ 0xff
								buf2=buf2 ^ 0x80
							elif(byteLength==2):
								buf2=buf1 ^ 0xffff
								buf2=buf2 ^ 0x8000
							elif(byteLength==3):
								buf2=buf1 ^ 0xffffff
								buf2=buf2 ^ 0x800000
							elif(byteLength==4):
								buf2=buf1 ^ 0xffffffff
								buf2=buf2 ^ 0x80000000
						else:
							fillLength=9
							if(byteLength==1):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xff
							elif(byteLength==2):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xffff
							elif(byteLength==3):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xffffff
							elif(byteLength==4):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xffffffff
						leftStr=leftStr+str(buf2).zfill(fillLength)							
				except Exception,e:
					raise Exception("error occur when unpack decimal type,error msg:%s"%(e))
			if(len(fracArray)==0):
				rightStr='0'
			else:
				try:
					for i in range(0,len(fracArray)):
						byteLength=fracArray[i]
						fillLength=0
						buf1=0
						buf2=0
						if(i==0 and len(intgArray)==0):
							if((len(fracArray)-1)!=i):
								fillLength=9
							else:
								fillLength=fracRight
							buf1=int(stream.read(byteLength).encode('hex'),16)
							if(byteLength==1):
								buf2=buf1 ^ 0xff
								buf2=buf2 ^ 0x80
							elif(byteLength==2):
								buf2=buf1 ^ 0xffff
								buf2=buf2 ^ 0x8000
							elif(byteLength==3):
								buf2=buf1 ^ 0xffffff
								buf2=buf2 ^ 0x800000
							elif(byteLength==4):
								buf2=buf1 ^ 0xffffffff
								buf2=buf2 ^ 0x80000000
						else:
							if((len(fracArray)-1)!=i):
								fillLength=9
							else:
								fillLength=fracRight
							if(byteLength==1):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xff
							elif(byteLength==2):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xffff
							elif(byteLength==3):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xffffff
							elif(byteLength==4):
								buf2=int(stream.read(byteLength).encode('hex'),16) ^ 0xffffffff
						rightStr=rightStr+str(buf2).zfill(fillLength)
				except Exception,e:
					raise Exception("error occur when unpack decimal type,error msg:%s,at position:%d"%(e,stream.tell()))					
				return ('-'+leftStr+'.'+rightStr)

class BinlogHeader:
	def __init__(self,stream):
		try:
			self.beginPos=stream.tell()
			#
			self.timestamp=Common.unpack4ByteInt(stream) 
			#event type
			self.eventType=Common.unpack1ByteInt(stream)
			#server_id
			self.serverId=Common.unpack4ByteInt(stream) 
			#event大小
			self.eventSize=Common.unpack4ByteInt(stream) 
			#下一个event的位置
			self.nextLogPos=Common.unpack4ByteInt(stream) 
			#flag
			self.flags=Common.unpack2ByteInt(stream) 
			self.headerLength=19
			self.endPos=self.beginPos+self.headerLength
		except Exception,e:
			raise Exception("error occur when read binlog header, error msg:%s,at position:%d"%(e,stream.tell()))				
				
class TableMap:
	def __init__(self,header,stream):
		try:
			self.header=header
			self.table_id=Common.unpack6ByteInt(stream)
			self.flags,=struct.unpack('h',stream.read(2))
			self.schemaLength,=struct.unpack('B',stream.read(1))
			self.schemaName=stream.read(self.schemaLength)
			stream.read(1)
			self.tableLength,=struct.unpack('B',stream.read(1))
			self.tableName=stream.read(self.tableLength)
			stream.read(1)
			self.columnCount=Common.unpackLenencInt(stream)
			#column type，
			self.columnTypeArr=struct.unpack((str(self.columnCount)+'B'),stream.read(self.columnCount))
			#how much bytes to store meta def
			self.metaDefLength=Common.unpackLenencInt(stream)
			self.metaDef=Common.metaDefOfMysqlType(self.columnTypeArr,stream)
			nullMaskLength=(self.columnCount+7)/8
			self.nullMask=struct.unpack((str(nullMaskLength)+'B'),stream.read(nullMaskLength))
		except Exception,e:
			raise Exception("error occur when read binlog table map event, error info:%s,at position:%d"%(e,stream.tell()))
class DescriptionEvent:
	def __init__(self,header,stream):
		try:
			self.header=header
			#binlog版本，
			self.binlogVersion,=struct.unpack('h',stream.read(2))
			self.description=''
			for i in range(0,50):
				buf,=struct.unpack('c',stream.read(1))
				if(buf!='\x00'):
					self.description=self.description+buf
			self.createTime,=struct.unpack('i',stream.read(4))
			self.eventHeaderLength=struct.unpack('B',stream.read(1))
			stream.seek(header.nextLogPos)
		except Exception,e:
			raise Exception("error occur when read binlog description event, error info:%s,at position:%d"%(e,stream.tell()))
class TableInfo:
	def __init__(self,schemaName,tableName,columnName):
		self.schemaName=schemaName
		self.tableName=tableName
		self.columnName=columnName

	def getInsertLeft(self):
		buf1="insert into `%s`.`%s`("%(self.schemaName,self.tableName)
		i=0
		for column in self.columnName:
			
			if(i!=(len(self.columnName)-1)):
				buf1=buf1+"`%s`,"%(column)
			else:
				buf1=buf1+"`%s`)values ("%(column)
			i=i+1
		return buf1
class RowEvent:
	def __init__(self,header,tableMap,stream,tableInfo):
		try:
			if(len(tableInfo.columnName)!=len(tableMap.metaDef)):
				raise Exception("fatal error:column numbers is not match")
			self.tableMap=tableMap
			self.header=header
			tmpHeader=header
			self.table_id=Common.unpack6ByteInt(stream)
			self.flag,=struct.unpack('h',stream.read(2))
			self.columnLength=Common.unpackLenencInt(stream)
			#null bitmap
			bitmapLength=(self.columnLength+7)/8
			self.data=[]
			self.dataBack=[]
			#write row or delete row
			if(header.eventType in (EventType.WRITE_ROWS_EVENTv1,EventType.WRITE_ROWS_EVENTv2)):
				stream.seek(stream.tell()+bitmapLength)
				
				while(stream.tell()<tmpHeader.nextLogPos):
					bitmap=struct.unpack(str(bitmapLength)+'B',stream.read(bitmapLength))
					idx=0
					nullMask=0
					insertRow=tableInfo.getInsertLeft()
					insertRowBack="delete from `%s`.`%s` where " %(tableInfo.schemaName,tableInfo.tableName)
					for mysqlType in tableMap.metaDef:
						nullMask=bitmap[idx/8]
						#not null
						if(((nullMask>>(idx%8))%2)==0):
							if(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_NEWDECIMAL or  mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DECIMAL ):
								buf1=Common.unpackDecimal(stream,mysqlType[1][0],mysqlType[1][1])
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %s,"%(insertRow,buf1)
								else:
									insertRow="%s %s);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%s and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%s;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0] in (MysqlTypeDef.MYSQL_TYPE_VARCHAR,MysqlTypeDef.MYSQL_TYPE_VAR_STRING)):
								buf1=Common.unpackVarString(stream,mysqlType[1])
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %r,"%(insertRow,buf1)
								else:
									insertRow="%s %r);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BLOB):
								buf1=Common.unpackBlob(stream,mysqlType[1])
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %r,"%(insertRow,buf1)
								else:
									insertRow="%s %r);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_STRING):
								if(mysqlType[1][0] in (247,248)):
									val1=0
									if(mysqlType[1][1]==1):
										val1=Common.unpack1ByteInt(stream)
									elif(mysqlType[1][1]==2):
										val1=Common.unpack2ByteInt(stream)
									else:
										raise Exception("invalid length of enum type or set type,pos:%d"%(stream.tell))
									#insert statement
									if(idx!=(len(tableMap.metaDef)-1)):
										insertRow="%s %d,"%(insertRow,val1)
									else:
										insertRow="%s %d);"%(insertRow,val1)
									#delete statement, to rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],val1)
									else:
										insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],val1)										
								else:		
									buf1=Common.unpackString(stream,mysqlType[1][1])
									#insert statement
									if(idx!=(len(tableMap.metaDef)-1)):
										insertRow="%s %r,"%(insertRow,buf1)
									else:
										insertRow="%s %r);"%(insertRow,buf1)
									#delete statement, to rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
									else:
										insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)									
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DOUBLE):
								buf1=Common.unpackDouble(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %s,"%(insertRow,buf1)
								else:
									insertRow="%s %s);"%(insertRow,buf1)
									#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%s and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%s;"%(insertRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_FLOAT):
								buf1=Common.unpackFloat(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %s,"%(insertRow,buf1)
								else:
									insertRow="%s %s);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%s and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%s;"%(insertRowBack,tableInfo.columnName[idx],buf1)			
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TINY):
								buf1=Common.unpack1ByteInt(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %d,"%(insertRow,buf1)
								else:
									insertRow="%s %d);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],buf1)									
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_SHORT):
								buf1=Common.unpack2ByteInt(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %d,"%(insertRow,buf1)
								else:
									insertRow="%s %d);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],buf1)														
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_INT24):
								buf1=Common.unpack3ByteInt(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %d,"%(insertRow,buf1)
								else:
									insertRow="%s %d);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],buf1)			
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONG):
								buf1=Common.unpack4ByteInt(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %d,"%(insertRow,buf1)
								else:
									insertRow="%s %d);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONGLONG):
								buf1=Common.unpack8ByteInt(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %d,"%(insertRow,buf1)
								else:
									insertRow="%s %d);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATETIME):
								buf1=Common.unpackDatetime(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %r,"%(insertRow,buf1)
								else:
									insertRow="%s %r);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATE):
								buf1=Common.unpackDate(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %r,"%(insertRow,buf1)
								else:
									insertRow="%s %r);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)									
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIMESTAMP):
								buf1=Common.unpackTimestamp(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %r,"%(insertRow,buf1)
								else:
									insertRow="%s %r);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIME):
								buf1=Common.unpackTime(stream)
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %r,"%(insertRow,buf1)
								else:
									insertRow="%s %r);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%r and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%r;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BIT):
								buf1=Common.unpackBit(stream,mysqlType[1])
								#insert statement
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRow="%s %d,"%(insertRow,buf1)
								else:
									insertRow="%s %d);"%(insertRow,buf1)
								#delete statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									insertRowBack="%s `%s`=%d and "%(insertRowBack,tableInfo.columnName[idx],buf1)
								else:
									insertRowBack="%s `%s`=%d;"%(insertRowBack,tableInfo.columnName[idx],buf1)	
							else:
								raise Exception("unknown mysql type:%s"%(mysqlType[0]))
						else:
							#insert statement
							if(idx!=(len(tableMap.metaDef)-1)):
								insertRow="%s %s"%(insertRow,'NULL,')
							else:
								insertRow="%s %s"%(insertRow,'NULL);')
							#delete statement, to rollback	
							if(idx!=(len(tableMap.metaDef)-1)):
								insertRowBack="%s `%s`=NULL and "%(insertRowBack,tableInfo.columnName[idx])
							else:
								insertRowBack="%s `%s`=NULL;"%(insertRowBack,tableInfo.columnName[idx])		
						idx=idx+1
					
					if(stream.tell()==tmpHeader.nextLogPos):
						header2=BinlogHeader(stream)
						if(header2.eventType in (EventType.WRITE_ROWS_EVENTv1,EventType.WRITE_ROWS_EVENTv2) ):
							tmpHeader=header2
							stream.seek(stream.tell()+8)
							Common.unpackLenencInt(stream)
							stream.seek(stream.tell()+bitmapLength)
						else:
							stream.seek(header2.beginPos)	

					self.data.append(insertRow)
					self.dataBack.append(insertRowBack)
			elif(header.eventType  in (EventType.DELETE_ROWS_EVENTv1,EventType.DELETE_ROWS_EVENTv2)):
				stream.seek(stream.tell()+bitmapLength)
				while(stream.tell()<tmpHeader.nextLogPos):
					bitmap=struct.unpack(str(bitmapLength)+'B',stream.read(bitmapLength))
					idx=0
					nullMask=0
					deleteRow="delete from `%s`.`%s` where " %(tableInfo.schemaName,tableInfo.tableName)
					deleteRowBack=tableInfo.getInsertLeft()
					for mysqlType in tableMap.metaDef:
						nullMask=bitmap[idx/8]
						#not null
						if(((nullMask>>(idx%8))%2)==0):
							if(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_NEWDECIMAL or  mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DECIMAL ):
								buf1=Common.unpackDecimal(stream,mysqlType[1][0],mysqlType[1][1])
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%s and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%s;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %s, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %s);"%(deleteRowBack,buf1)	
							elif(mysqlType[0] in (MysqlTypeDef.MYSQL_TYPE_VARCHAR,MysqlTypeDef.MYSQL_TYPE_VAR_STRING)):
								
								buf1=Common.unpackVarString(stream,mysqlType[1])
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %r, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %r);"%(deleteRowBack,buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BLOB):
								buf1=Common.unpackBlob(stream,mysqlType[1])
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %r, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %r);"%(deleteRowBack,buf1)											
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_STRING):
								if(mysqlType[1][0] in (247,248)):
									val1=0
									if(mysqlType[1][1]==1):
										val1=Common.unpack1ByteInt(stream)
									elif(mysqlType[1][1]==2):
										val1=Common.unpack2ByteInt(stream)
									else:
										raise Exception("invalid length of enum type or set type,pos:%d"%(stream.tell))
									#delete statement
									if(idx!=(len(tableMap.metaDef)-1)):
										deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],val1)
									else:
										deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],val1)
									#insert statement, for rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										deleteRowBack="%s %d, "%(deleteRowBack,val1)
									else:
										deleteRowBack="%s %d);"%(deleteRowBack,val1)							
								else:
									buf1=Common.unpackString(stream,mysqlType[1][1])
									#delete statement
									if(idx!=(len(tableMap.metaDef)-1)):
										deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
									else:
										deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
									#insert statement, for rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										deleteRowBack="%s %r, "%(deleteRowBack,buf1)
									else:
										deleteRowBack="%s %r);"%(deleteRowBack,buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DOUBLE):
								buf1=Common.unpackDouble(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%s and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%s;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %s, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %s);"%(deleteRowBack,buf1)											
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_FLOAT):
								buf1=Common.unpackFloat(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%s and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%s;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %s, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %s);"%(deleteRowBack,buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TINY):
								buf1=Common.unpack1ByteInt(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %d, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %d);"%(deleteRowBack,buf1)								
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_SHORT):
								buf1=Common.unpack2ByteInt(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %d, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %d);"%(deleteRowBack,buf1)												
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_INT24):
								buf1=Common.unpack3ByteInt(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %d, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %d);"%(deleteRowBack,buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONG):
								buf1=Common.unpack4ByteInt(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %d, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %d);"%(deleteRowBack,buf1)
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONGLONG):
								buf1=Common.unpack8ByteInt(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %d, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %d);"%(deleteRowBack,buf1)
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATETIME):
								buf1=Common.unpackDatetime(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %r, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %r);"%(deleteRowBack,buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATE):
								buf1=Common.unpackDate(stream)
										#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %r, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %r);"%(deleteRowBack,buf1)								
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIMESTAMP):
								buf1=Common.unpackTimestamp(stream)
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %r, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %r);"%(deleteRowBack,buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIME):
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%r and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%r;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %r, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %r);"%(deleteRowBack,buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BIT):
								buf1=Common.unpackBit(stream,mysqlType[1])
								#delete statement
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRow="%s `%s`=%d and "%(deleteRow,tableInfo.columnName[idx],buf1)
								else:
									deleteRow="%s `%s`=%d;"%(deleteRow,tableInfo.columnName[idx],buf1)
								#insert statement, for rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									deleteRowBack="%s %d, "%(deleteRowBack,buf1)
								else:
									deleteRowBack="%s %d);"%(deleteRowBack,buf1)								
							
							else:
								raise Exception("unknown mysql type:%s"%(mysqlType[0]))
						else:
							#insert statement
							if(idx!=(len(tableMap.metaDef)-1)):
								deleteRow="%s `%s`=NULL and "%(deleteRow,tableInfo.columnName[idx])
							else:
								deleteRow="%s `%s`=NULL;"%(deleteRow,tableInfo.columnName[idx])
							#delete statement, to rollback	
							if(idx!=(len(tableMap.metaDef)-1)):
								deleteRowBack="%s NULL,"%(deleteRowBack)
							else:
								deleteRowBack="%s NULL);"%(deleteRowBack)
						idx=idx+1
						
					if(stream.tell()==tmpHeader.nextLogPos):
						header2=BinlogHeader(stream)
						if(header2.eventType in (EventType.DELETE_ROWS_EVENTv1,EventType.DELETE_ROWS_EVENTv2)):
							tmpHeader=header2
							stream.seek(stream.tell()+8)
							Common.unpackLenencInt(stream)
							stream.seek(stream.tell()+bitmapLength)
							
						else:
							stream.seek(header2.beginPos)
					self.data.append(deleteRow)
					self.dataBack.append(deleteRowBack)			
			elif(header.eventType in (EventType.UPDATE_ROWS_EVENTv1,EventType.UPDATE_ROWS_EVENTv2)):
				#old bitmap
				stream.seek(stream.tell()+bitmapLength)
				stream.seek(stream.tell()+bitmapLength)
				while(stream.tell()<tmpHeader.nextLogPos):
					bitmap=struct.unpack(str(bitmapLength)+'B',stream.read(bitmapLength))
					idx=0
					nullMask=0

					updateRow="update `%s`.`%s` set " % (tableInfo.schemaName,tableInfo.tableName)
					tmpRow=" where "  
					updateRowBack="update `%s`.`%s` set " %(tableInfo.schemaName,tableInfo.tableName)
					tmpRowBack=" where "
					#row data before update
					for mysqlType in tableMap.metaDef:
						nullMask=bitmap[idx/8]
						#not null
						if(((nullMask>>(idx%8))%2)==0):
							if(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_NEWDECIMAL or  mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DECIMAL ):
								buf1=Common.unpackDecimal(stream,mysqlType[1][0],mysqlType[1][1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%s and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%s;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%s,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%s "%(updateRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0] in (MysqlTypeDef.MYSQL_TYPE_VARCHAR,MysqlTypeDef.MYSQL_TYPE_VAR_STRING)):
								buf1=Common.unpackVarString(stream,mysqlType[1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BLOB):
								buf1=Common.unpackBlob(stream,mysqlType[1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_STRING):
								if(mysqlType[1][0] in (247,248)):
									val1=0
									if(mysqlType[1][1]==1):
										val1=Common.unpack1ByteInt(stream)
									elif(mysqlType[1][1]==2):
										val1=Common.unpack2ByteInt(stream)
									else:
										raise Exception("invalid length of enum type or set type,pos:%d"%(stream.tell))
									#update statement
									if(idx!=(len(tableMap.metaDef)-1)):
										tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],val1)
									else:
										tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],val1)
									#update statement, to rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],val1)
									else:
										updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],val1)							
								else:
									buf1=Common.unpackString(stream,mysqlType[1][1])
									#update statement
									if(idx!=(len(tableMap.metaDef)-1)):
										tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
									else:
										tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
									#update statement, to rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
									else:
										updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)									
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DOUBLE):
								buf1=Common.unpackDouble(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%s and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%s;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%s,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%s "%(updateRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_FLOAT):
								buf1=Common.unpackFloat(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%s and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%s;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%s,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%s "%(updateRowBack,tableInfo.columnName[idx],buf1)	
										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TINY):
								buf1=Common.unpack1ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],buf1)	
																		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_SHORT):
								buf1=Common.unpack2ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],buf1)														
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_INT24):
								buf1=Common.unpack3ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONG):
								buf1=Common.unpack4ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONGLONG):
								buf1=Common.unpack8ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATETIME):
								buf1=Common.unpackDatetime(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATE):
								buf1=Common.unpackDate(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)								
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIMESTAMP):
								buf1=Common.unpackTimestamp(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIME):
								buf1=Common.unpackTime(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%r and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%r;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%r,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%r "%(updateRowBack,tableInfo.columnName[idx],buf1)
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BIT):
								buf1=Common.unpackBit(stream,mysqlType[1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRow="%s `%s`=%d and"%(tmpRow,tableInfo.columnName[idx],buf1)
								else:
									tmpRow="%s `%s`=%d;"%(tmpRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRowBack="%s `%s`=%d,"%(updateRowBack,tableInfo.columnName[idx],buf1)
								else:
									updateRowBack="%s `%s`=%d "%(updateRowBack,tableInfo.columnName[idx],buf1)									
							
							else:
								raise Exception("unknown mysql type:%s"%(mysqlType[0]))
						else:
							#update statement
							if(idx!=(len(tableMap.metaDef)-1)):
								tmpRow="%s `%s`=NULL and "%(tmpRow,tableInfo.columnName[idx])
							else:
								tmpRow="%s `%s`=NULL;"%(tmpRow,tableInfo.columnName[idx])
							#update statement, to rollback	
							if(idx!=(len(tableMap.metaDef)-1)):
								updateRowBack="%s `%s`=NULL,"%(updateRowBack,tableInfo.columnName[idx])
							else:
								updateRowBack="%s `%s`=NULL "%(updateRowBack,tableInfo.columnName[idx])		
						idx=idx+1					
						
					bitmap=struct.unpack(str(bitmapLength)+'B',stream.read(bitmapLength))
					idx=0
					nullMask=0
					tmpRowBack=" where "
					updateRow="update `%s`.`%s` set " % (tableInfo.schemaName,tableInfo.tableName)
					#row data after update
					for mysqlType in tableMap.metaDef:
						nullMask=bitmap[idx/8]
						#not null
						if(((nullMask>>(idx%8))%2)==0):
							if(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_NEWDECIMAL or  mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DECIMAL ):
								buf1=Common.unpackDecimal(stream,mysqlType[1][0],mysqlType[1][1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%s ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%s "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%s and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%s ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0] in (MysqlTypeDef.MYSQL_TYPE_VARCHAR,MysqlTypeDef.MYSQL_TYPE_VAR_STRING)):
								buf1=Common.unpackVarString(stream,mysqlType[1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BLOB):
								buf1=Common.unpackBlob(stream,mysqlType[1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)											
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_STRING):
								if(mysqlType[1][0] in (247,248)):
									val1=0
									if(mysqlType[1][1]==1):
										val1=Common.unpack1ByteInt(stream)
									elif(mysqlType[1][1]==2):
										val1=Common.unpack2ByteInt(stream)
									else:
										raise Exception("invalid length of enum type or set type,pos:%d"%(stream.tell))
									#update statement
									if(idx!=(len(tableMap.metaDef)-1)):
										updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],val1)
									else:
										updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],val1)
									#update statement, to rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],val1)
									else:
										tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],val1)							
								else:
									buf1=Common.unpackString(stream,mysqlType[1][1])
									#update statement
									if(idx!=(len(tableMap.metaDef)-1)):
										updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
									else:
										updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
									#update statement, to rollback	
									if(idx!=(len(tableMap.metaDef)-1)):
										tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
									else:
										tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DOUBLE):
								buf1=Common.unpackDouble(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%s ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%s "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%s and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%s ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)										
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_FLOAT):
								buf1=Common.unpackFloat(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%s ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%s "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%s and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%s ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TINY):
								buf1=Common.unpack1ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)							
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_SHORT):
								buf1=Common.unpack2ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)														
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_INT24):
								buf1=Common.unpack3ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)		
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONG):
								buf1=Common.unpack4ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)			
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_LONGLONG):
								buf1=Common.unpack8ByteInt(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATETIME):
								buf1=Common.unpackDatetime(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)									
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_DATE):
								buf1=Common.unpackDate(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)							
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIMESTAMP):
								buf1=Common.unpackTimestamp(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_TIME):
								buf1=Common.unpackTime(stream)
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%r ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%r "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%r and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%r ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)	
							elif(mysqlType[0]==MysqlTypeDef.MYSQL_TYPE_BIT):
								buf1=Common.unpackBit(stream,mysqlType[1])
								#update statement
								if(idx!=(len(tableMap.metaDef)-1)):
									updateRow="%s `%s`=%d ,"%(updateRow,tableInfo.columnName[idx],buf1)
								else:
									updateRow="%s `%s`=%d "%(updateRow,tableInfo.columnName[idx],buf1)
								#update statement, to rollback	
								if(idx!=(len(tableMap.metaDef)-1)):
									tmpRowBack="%s `%s`=%d and"%(tmpRowBack,tableInfo.columnName[idx],buf1)
								else:
									tmpRowBack="%s `%s`=%d ;"%(tmpRowBack,tableInfo.columnName[idx],buf1)	
							else:
								raise Exception("unknown mysql type:%s"%(mysqlType[0]))
						else:
							#insert statement
							if(idx!=(len(tableMap.metaDef)-1)):
								updateRow="%s `%s`=NULL,"%(updateRow,tableInfo.columnName[idx])
							else:
								updateRow="%s `%s`=NULL "%(updateRow,tableInfo.columnName[idx])
							#delete statement, to rollback	
							if(idx!=(len(tableMap.metaDef)-1)):
								tmpRowBack="%s `%s`=NULL and "%(tmpRowBack,tableInfo.columnName[idx])
							else:
								tmpRowBack="%s `%s`=NULL;"%(tmpRowBack,tableInfo.columnName[idx])		
						idx=idx+1
					if(stream.tell()==tmpHeader.nextLogPos):
						header2=BinlogHeader(stream)
						if(header2.eventType in (EventType.UPDATE_ROWS_EVENTv1,EventType.UPDATE_ROWS_EVENTv2)):
							tmpHeader=header2
							stream.seek(stream.tell()+8)
							Common.unpackLenencInt(stream)
							stream.seek(stream.tell()+bitmapLength*2)
						else:
							stream.seek(header2.beginPos)							
					
					updateRow=updateRow+tmpRow
					updateRowBack=updateRowBack+tmpRowBack
					self.data.append(updateRow)
					self.dataBack.append(updateRowBack)
			#reverse dataBack ,replay  order by DESC
			self.dataBack.reverse()
		except Exception,e:
			#raise Exception("error occur when read write_row event, error info:%s,at position:%d"%(e,stream.tell()))	
			raise e
#query event,only check BEGIN COMMIT ROLLBACK statement
class QueryEvent:
	def __init__(self,header,stream):
		try:
			self.header=header
			self.thread_id=Common.unpack4ByteInt(stream)
			self.execTime=Common.unpack4ByteInt(stream)
			self.schemaLength,=struct.unpack('B',stream.read(1))
			self.errorCode,=struct.unpack('h',stream.read(2))
			self.statusVarLength,=struct.unpack('h',stream.read(2))		
			#skip status var and schema,only check statement
			stream.seek(stream.tell()+self.statusVarLength+self.schemaLength)
			stream.read(1)
			self.statement=stream.read(header.nextLogPos-stream.tell())
		except Exception,e:
			raise e
	
	def  isBegin(self):
		if(self.statement in ('BEGIN','begin')):
			return True
		else:
			return False	
	def  isCommit(self):
		if(self.statement in ('COMMIT','commit')):
			return True
		else:
			return False
	def isRollback(self):
		if(self.statement in ('rollback','ROLLBACK')):
			return True
		else:
			return False		
#every operation is a transaction,no matter  what the engine is		

class XIDEvent:
	def __init__(self,header,stream):
		self.xid=Common.unpack8ByteInt(stream)
				
class Transaction:
	def __init__(self,stream,tableInfo,mode='desc',binlogSize=0):
		self.data=[]
		lastQueryStatement=["COMMIT;"]
		#check all row event of the tableInfo,until find commit statement or  
		try:
			while(True):
				header=BinlogHeader(stream)
				#find table map event,if not check query event
				if(header.eventType!=EventType.TABLE_MAP_EVENT):
					if(header.eventType!=EventType.QUERY_EVENT and header.eventType!=EventType.XID_EVENT):
						stream.seek(header.nextLogPos)
						continue
					elif(header.eventType==EventType.XID_EVENT):
						tmpXIDEvent=XIDEvent(header,stream)
						lastQueryStatement=["COMMIT;"]
						break
					else:
						tmpQueryEvent=QueryEvent(header,stream)
						if(tmpQueryEvent.isCommit()):
							lastQueryStatement=["COMMIT;"]
							break
						elif(tmpQueryEvent.isRollback()):
							lastQueryStatement=["ROLLBACK;"]
							break
						elif(tmpQueryEvent.isBegin()):
							stream.seek(tmpQueryEvent.header.beginPos)
							lastQueryStatement=["ROLLBACK;"]
							break
						else:
							stream.seek(header.nextLogPos)
							continue
				else:
					tmpMap=TableMap(header,stream)
					#correct table name and schema name
					if(tmpMap.schemaName==tableInfo.schemaName and tmpMap.tableName==tableInfo.tableName):
						#check event type, only for row event
						header2=BinlogHeader(stream)

						if(header2.eventType not in (EventType.UPDATE_ROWS_EVENTv1,EventType.WRITE_ROWS_EVENTv1,EventType.DELETE_ROWS_EVENTv1,EventType.UPDATE_ROWS_EVENTv2,EventType.WRITE_ROWS_EVENTv2,EventType.DELETE_ROWS_EVENTv2)):
							stream.seek(header2.nextLogPos)		
							continue
						else:
							tmpRowEvent=RowEvent(header2,tmpMap,stream,tableInfo)
							if(mode=='desc'):
								self.data=tmpRowEvent.dataBack+self.data
							else:
								self.data=self.data+tmpRowEvent.data
							del tmpRowEvent
							gc.collect()
					#not correct table name and schema name
					else:
						stream.seek(header.nextLogPos)
						continue
			
			
		except Exception,e:
			#end of this binlog file,but not the end of the transaction,so we add rollback statement
			if(stream.tell()==binlogSize):
				lastQueryStatement=["ROLLBACK;"]
			else:
				raise e
		if(mode=='desc'):	
			self.data=["#pos:%d\nBEGIN;"%(header.beginPos)]+self.data+lastQueryStatement	
		else:
			self.data=["#pos:%d\nBEGIN;"%(header.beginPos)]+self.data+lastQueryStatement			
		#filter useless "begin;commit;"
		if(len(self.data)==2):
			self.data=[]
		
		
		#print self.data
class Binlog:
	def __init__(self,filepath,stream,mode,tabelInfo,startTime=None,stopTime=None,binlogSize=0):
		#step1 skip first 4 byte
		stream.seek(4)
		#step2  first header
		self.data=[]
		try:
			while(stream.tell()<binlogSize):
				header=BinlogHeader(stream)

				if(startTime is not None):
					if(header.timestamp<startTime):
						stream.seek(header.nextLogPos)
						continue
				if(stopTime is not None):
					if(header.timestamp>stopTime):
						break
				#last event of this binlog,rotate to next binlog file
				if(header.eventType==EventType.ROTATE_EVENT):
					break
				if(header.eventType!=EventType.QUERY_EVENT):
					stream.seek(header.nextLogPos)
					continue
				tmpQueryEvent=QueryEvent(header,stream)	
				if(tmpQueryEvent.isBegin()):
					tran=Transaction(stream,tableInfo,mode)
					if(mode=='desc'):
						self.data=tran.data+self.data
					else:
						self.data=self.data+tran.data
					del tran
					gc.collect()
				else:
					continue
				
		except Exception,e:
			raise e
			

def usage():
	sys.stdout.write("usage:binlogback.py -f mysql-bin.000001 -m dsec -B unit -t t1 -c c1,c2,c3\nlimits:the table must have a primary key,and no ddl changes of all columns\n")
	sys.stdout.write("-f,--file             binlog path,multi file split by ',',do not support regex of path\n")	
	sys.stdout.write("-m,--mode             asc or desc,default is desc,recovering data need set mode=desc\n")	
	sys.stdout.write("-B,--database         schema name\n")
	sys.stdout.write("-t,--table            table name\n")
	sys.stdout.write("-c,--columns          all columns:c1,c2,c3...\n")
	sys.stdout.write("-b,--begin-datetime   begin time,yyyy-MM-DD HH:mm:ss\n")
	sys.stdout.write("-s,--end-datetime     end time,yyyy-MM-DD HH:mm:ss\n")
	sys.stdout.write("-h,--help             usage\n")		
	sys.stdout.write("-v,--version          print version\n")	

		
#test
if	__name__=='__main__':

	if(len(sys.argv)<=1):
		usage()
		sys.exit(2)


	opts=None
	args=None
	try:                                
		opts,args = getopt.getopt(sys.argv[1:], "hf:vm:B:t:c:b:e:", ["help", "file=","version","mode=","database=","table=","columns=","begin-datetime=","end-datetime="]) 
	except getopt.GetoptError:           
		usage()                         
		sys.exit(2)
	file=None
	mode="desc"
	schemaName=None
	table=None
	columns=None
	startTime=None
	stopTime=None
	for opt, arg in opts:                
		if opt in ("-h", "--help"):      
			usage()                     
			sys.exit()                  
		elif opt in ("-f","--file"):  
			file=arg.split(',')
			tmp=[]
			tmp2=[]
			try:
				#check file path and format
				for filename in file:
					tmpfile=os.path.abspath(filename)
					if(os.path.isfile(tmpfile)):
						stream=open(tmpfile,'rb')
						headerFlag=stream.read(4)
						#is not a mysql binlog
						if(headerFlag!='\xfebin'):
							sys.stderr.write("%s is not a binlog file"%(tmpfile))							
							stream.close()
							sys.exit(2)
						
						header=BinlogHeader(stream)
						tmpDes=DescriptionEvent(header,stream)
						#check binlog version
						if(tmpDes.binlogVersion!=4):
							sys.stderr.write("binlog version:%d is not supported yet\n"%(tmpDes.binlogVersion))							
							stream.close()
							sys.exit(2)
						
						tmp.append((header.timestamp,tmpfile))

					else:
						sys.stderr.write("%s file is not found"%(tmpfile))
						sys.exit(2)
				#same file should be removed
				tmp=set(tmp)
				tmp=list(tmp)
				#sort file by binlog header timestamp asc
				for i in range (0,len(tmp)):
					for j in range(0,len(tmp)-1-i):
						#if timestamp is the same,sort by mysql-bin.xxxxxx
						if(tmp[j][0]==tmp[j+1][0]):
							suffix1=(tmp[j][1]).split('.')[-1]
							suffix2=(tmp[j+1][1]).split('.')[-1]
							if(suffix1>suffix1):
								buf=tmp[j]
								tmp[j]=tmp[j+1]
								tmp[j+1]=buf					
						if(tmp[j][0]>tmp[j+1][0]):
							buf=tmp[j]
							tmp[j]=tmp[j+1]
							tmp[j+1]=buf
				for i in tmp:
					tmp2.append(i[1])
				file=tmp2
			except Exception,e:
				sys.stderr.write("err occur when open binlog file,error msg:%s\n"%(e))
				sys.exit(2)
		elif opt in ("-m", "--mode"): 
			if (arg not in ("asc","desc")):
				sys.stderr.write("unknown mode %s\n"%(arg))
				sys.exit(2)
			mode=arg
		elif opt in ("-B","--database"):
			schemaName=arg
		elif opt in ("-t","--table"):
			table=arg
		elif opt in ("-c","--columns"):
			columns=arg.split(",")
		elif opt in ("-b","--begin-datetime"):
			try:
				startTime=int(time.mktime(time.strptime(arg,"%Y-%m-%d %H:%M:%S")))
			except Exception:
				sys.stderr.write("illegal time format\n")
				sys.exit(2)
		elif opt in ("-e","--end-datetime"):
			try:
				stopTime=int(time.mktime(time.strptime(arg,"%Y-%m-%d %H:%M:%S")))
			except Exception:
				sys.stderr.write("illegal time format\n")
				sys.exit(2)
		elif opt in ("-v","--version"):
			sys.stdout.write(VERSION+"\n")
			sys.exit()
		else:
			usage()
			sys.exit(2)

	if(file is None or schemaName is None or table is None or columns is None):
		sys.stderr.write("--file --database --table --columns is needed\n")
		usage()
		sys.exit(2)
	
	tableInfo=TableInfo(schemaName,table,columns)
	#step 1;
	
	if(mode=='desc'):
		file.reverse()
	
	for filepath in file:
		try:
			stream=open(filepath,'rb')
			binlogSize=os.path.getsize(filepath)
			binlog=Binlog(filepath,stream,mode,tableInfo,startTime,stopTime,binlogSize)
			for statement in binlog.data:
				sys.stdout.write(statement+"\n")
				pass
			stream.close()
		except Exception,e:
			sys.stderr.write("error occur when read binlog,error msg:%s ,log pos:%d\n"%(e,stream.tell()))		
			sys.exit(2)
	
			
		
			
		
	
	
		