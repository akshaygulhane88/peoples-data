from pyspark.sql.functions import explode, col, lit
import pyspark.sql.types as T
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from . import pysparkfuncs    
import pandas as pd                        

class JsonFlattenParser():

    def __init__(self):
        self.parse = None
        self.seperator = None
        
    
    @staticmethod
    def parse_aux(path: str, aux: str) -> Tuple[str, str]:
        '''
        Parses the child elements in the nested Json struct and returns the relative path of the child element
        :param path: root path
        :param aux: path of the child element
        :return tuple of child path and datatype
        '''
        path_child: str
        dtype: str
        if ":" in aux:
            if (aux.startswith("http")):
                if (len(aux.split(":")) > 2):
                    dtype, path_child = aux[::-1].split(sep=":", maxsplit=1)
                    dtype = dtype[::-1]
                    path_child = path_child[::-1]
                else:
                    path_child = "element"
            else:
                path_child, dtype = aux.split(sep=":", maxsplit=1)
        else:
            path_child = "element"
        return f'{path}["{path_child}"]', dtype
        
     
    @staticmethod
    def flatten_struct_column(path: str, dtype: str) -> List[Tuple[str, str]]:
        '''
        Returns flattened struct column
        :param path: path of the struct column with respect to root
        :param dtype: datatype of the column
        :return List of tuples of columns
        '''
        dtype = dtype[7:-1]  # Cutting off "struct<" and ">"
        cols: List[Tuple[str, str]] = []
        struct_acc: int = 0
        path_child: str
        dtype_child: str
        aux: str = ""
        for c, i in zip(dtype, range(len(dtype), 0, -1)):  # Zipping a descendant ID for each letter
            if ((c == ",") and (struct_acc == 0)) or (i == 1):
                if i == 1:
                    aux += c
                path_child, dtype_child = JsonFlattenParser.parse_aux(path=path, aux=aux)
                if pysparkfuncs.is_struct(dtype=dtype_child):
                    cols += JsonFlattenParser.flatten_struct_column(path=path_child, dtype=dtype_child)  # Recursion
                elif pysparkfuncs.is_array(dtype=dtype):
                    cols.append((path, "array"))
                else:
                    cols.append((path_child, dtype_child))
                aux = ""
            elif c == "<":
                aux += c
                struct_acc += 1
            elif c == ">":
                aux += c
                struct_acc -= 1
            else:
                aux += c
        return cols
    
    @staticmethod
    def flatten_struct_dataframe(df: DataFrame,
                                  explode_outer: bool = True,
                                  explode_pos: bool = True) -> List[Tuple[str, str, str]]:
        '''
        Returns flattened struct dataframe
        :param path: path of the struct column with respect to root
        :param dtype: datatype of the column
        :return List of tuples of columns
        '''
        explode: str = "EXPLODE_OUTER" if explode_outer is True else "EXPLODE"
        explode = f"POS{explode}" if explode_pos is True else explode
        cols: List[Tuple[str, str]] = []
        for path, dtype in df.dtypes:
            if pysparkfuncs.is_struct(dtype=dtype):
                cols += JsonFlattenParser.flatten_struct_column(path=path, dtype=dtype)
            elif pysparkfuncs.is_array(dtype=dtype):
                cols.append((path, "array"))
            elif pysparkfuncs.is_map(dtype=dtype):
                cols.append((path, "map"))
            else:
                cols.append((path, dtype))
        cols_exprs: List[Tuple[str, str, str]] = []
        expr: str
        for path, dtype in cols:
            path_under = path.replace('.', '_').replace('-','_').replace(':','_').replace('/','_') \
                                   .replace('#','_') \
                                   .replace('"','_').replace('[','_').replace(']','_').replace(' ','_')
            if pysparkfuncs.is_array(dtype):
                if explode_pos:
                    expr = f"{explode}({path}) AS ({path_under}_pos, {path_under})"
                else:
                    expr = f"{explode}({path}) AS {path_under}"
            elif pysparkfuncs.is_map(dtype):
                if explode_pos:
                    expr = f"{explode}({path}) AS ({path_under}_pos, {path_under}_key, {path_under}_value)"
                else:
                    expr = f"{explode}({path}) AS ({path_under}_key, {path_under}_value)"
            else:
                path_encoded = path.replace('.', '_').replace('-','_').replace(':','_').replace('/','_') \
                                   .replace('#','_') \
                                   .replace('"','_').replace('[','_').replace(']','_').replace(' ','_')
                expr = f"{path} AS {path_encoded}"
            cols_exprs.append((path, dtype, expr))
        print("cols_exprs in func flatten_struct_dataframe is :",cols_exprs)
        return cols_exprs
    
    @staticmethod
    def build_name(name: str, expr: str) -> str:
        '''
        Builds column names with respect to root concatened with an underscore
        :param name: name of the actual column
        :param expr: column names with respect to root
        :return columns concatenated with respect to root
        '''
        suffix: str = expr[expr.find("(") + 1:expr.find(")")]
        return f"{name}_{suffix}".replace(".", "_").replace('-','_').replace(':','_').replace('/','_').replace('#','_').replace('"','_').replace('[','_').replace(']','_').replace(' ','_')
    
    def flatten(self,dataframe: DataFrame,
                explode_outer: bool = True,
                explode_pos: bool = True,
                name: str = "root") -> Dict[str, DataFrame]:
        """
        Convert a complex nested DataFrame in one (or many) flat DataFrames.

        If a columns is a struct it is flatten directly.
        If a columns is an array or map, then child DataFrames are created in different granularities.

        :param dataframe: Spark DataFrame
        :param explode_outer: Should we preserve the null values on arrays?
        :param explode_pos: Create columns with the index of the ex-array
        :param name: The name of the root Dataframe
        :return: A dictionary with the names as Keys and the DataFrames as Values
        """
        empty_root_df_flag = False
        cols_exprs: List[Tuple[str, str, str]] = JsonFlattenParser.flatten_struct_dataframe(df=dataframe,
                                                                                 explode_outer=explode_outer,
                                                                                 explode_pos=explode_pos)
        print("col_exprs is:",cols_exprs)
        exprs_arr: List[str] = [x[2] for x in cols_exprs if pysparkfuncs.is_array_or_map(x[1])]
        print("exprs_arr is :",exprs_arr)
        exprs: List[str] = [x[2] for x in cols_exprs if not pysparkfuncs.is_array_or_map(x[1])]
        print("exprs is :",exprs)
        if len(exprs)==0:
            empty_root_df_flag =True
        dfs: Dict[str, DataFrame] = {name: dataframe.selectExpr(exprs)}
        #print("dfs is :",dfs)
        #return dfs
        exprs = [x[2] for x in cols_exprs if not pysparkfuncs.is_array_or_map(x[1]) and not x[0].endswith("_pos")]
        print("exprs 2nd time is :",exprs)
        for expr in exprs_arr:
            print("expr is :", expr)
            df_arr = dataframe.selectExpr(exprs + [expr])
            print("df_arr is:",df_arr.show())
            name_new: str = JsonFlattenParser.build_name(name=name, expr=expr)
            print("name_new is :", name_new)
            dfs_new = self.flatten(dataframe=df_arr,
                                    explode_outer=explode_outer,
                                    explode_pos=explode_pos,
                                    name=name_new)
            print("type of dfs_new is :", type(dfs_new))
            print("dfs is :",dfs)
            print("dfs_new is :",dfs_new)
            if empty_root_df_flag:
                dfs = {**dfs_new}
            else:
                dfs = {**dfs, **dfs_new}
                print("dfs final is :",dfs)
        return dfs
    
    '''
    def getflatteneddataframe(self,dictdfs:Dict[str, DataFrame])->DataFrame:
        
        :param dictdfs: dictionary of dataframes
        :return flattened datframe
                               
        if len(dictdfs.keys())==1:
            flatteneddf = list(dictdfs.values())[0]
        else:
            for key,value in dictdfs.items():
                if key=='root':
                    tempdf=value
                    mergedf=tempdf
                else:
                    mergedf = mergedf.join(value,list(tempdf.columns))
                    flatteneddf = mergedf
        return flatteneddf
    '''
    
    def getflatteneddataframe(self,spark, dictdfs:Dict[str, DataFrame])->DataFrame:
        '''
        :param dictdfs: dictionary of dataframes
        :return flattened datframe
        ''' 
        pandasDF=pd.io.json.json_normalize(dictdfs)
        pandasDF = pandasDF.dropna(axis='columns')
        flatteneddf=spark.createDataFrame(pandasDF) 
        flatteneddf.printSchema()
        flatteneddf.show(100,truncate=False)
        return flatteneddf
            
  