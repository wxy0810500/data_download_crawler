# substance-ide

## 数据信息 

1. 数据源：ide
2. 数据格式:xml
3. 导入的原始数据表：IDE、YY、CALC、EXTID、SOVERW、SUBLINK、CLPHASE
4. 每个源文件包含500个substance，每个都对应一个IDE.XRN，可视为uniqueId.
每个substance可能包含yy,calc,extid,soverw,sublink,clphase子结构，也可能没有.每个子结构对应上述的一张表,每张表中保存了IDE.XRN作为key.
IDE表的status字段用于记录新增的数据。新增的数据status为1，在处理过之后应改为0.
5. 最终要导入到GHDDI_sList中

## 处理过程

1. 将原始数据文件整体放入到/data/file/ide/substance-import/文件夹下,注意备份这些源文件;
2. 调用java程序,main函数即为入口,从指定文件夹读取xml文件,逐个解析,插入到各原始数据表中;
3. 用sql语句查询IDE中status=1的IDE\_XRN,放入IDE\_NEW表中:
```
insert into ghddi.IDE_NEW SELECT IDE_XRN FROM ghddi.IDE where status = 1;
```
4. 根据IDE\_NEW表中的新IDE,联查IDE及EXTID表,将指定字段的内容一并新增入GHDDI\_sList表中:

```
insert into GHDDI_SList (IDE_XRN, IDE_XPR, EXTID_CASRN, EXTID_INCHI)
SELECT 
    NIDE.IDE_XRN, IDE.IDE_XPR, EXT.EXTID_CASRN, EXT.EXTID_INCHI
FROM
    (SELECT 
        IDE_XRN
    FROM
        IDE_NEW) AS NIDE
        LEFT JOIN
    ghddi.IDE AS IDE ON NIDE.IDE_XRN = IDE.IDE_XRN
        LEFT JOIN
    ghddi.EXTID AS EXT ON NIDE.IDE_XRN = EXT.IDE_XRN
```

5. 读取yy表中的YY\_STR,利用rdkit转化成smiles,后保存到GHDDI\_sList的对应record中.调用{GHDDI}/python/ide/transYY\_STRtoSimlesYY.threadtest.py进行转化.其中,{GHDDI}/python/ide/log.txt记录了进度数据,{GHDDI}/python/ide/error.txt中记录了抛出异常的IDE\_XRN.
6. 利用sql语句查询GHDDI_sList中新增的数据量是否和YY中的有效数据量相等.


