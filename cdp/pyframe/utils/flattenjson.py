from pyspark.sql.functions import explode, col, lit, udf, split
import pyspark.sql.types as T
from typing import Dict, List, Tuple
from pyspark.sql import DataFrame
from pyspark.sql import types
from pyspark.sql.types import ArrayType, StructType, StringType


class JsonFlattenParser():

    def __init__(self):
        self.parse = None
        self.seperator = None
    
    @staticmethod
    def is_struct(dtype: str) -> bool:
        '''
        Returns boolean value after checking whether the value's datatype is a struct
        :param dtype: datatype of value
        :return boolean true or false
        '''
        return True if dtype.startswith("struct") else False
    
    @staticmethod
    def is_array(dtype: str) -> bool:
        '''
        Returns boolean value after checking whether the value's datatype is an array
        :param dtype: datatype of value
        :return boolean true or false
        '''
        return True if dtype.startswith("array") else False
    
    @staticmethod
    def is_map(dtype: str) -> bool:
        '''
        Returns boolean value after checking whether the value's datatype is a map
        :param dtype: datatype of value
        :return boolean true or false
        '''
        return True if dtype.startswith("map") else False
    
    @staticmethod
    def is_array_or_map(dtype: str) -> bool:
        '''
        Returns boolean value after checking whether the value's datatype is a map or an array
        :param dtype: datatype of value
        :return boolean true or false
        '''
        return True if (dtype.startswith("array") or dtype.startswith("map")) else False
        
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
                if JsonFlattenParser.is_struct(dtype=dtype_child):
                    cols += JsonFlattenParser.flatten_struct_column(path=path_child, dtype=dtype_child)  # Recursion
                elif JsonFlattenParser.is_array(dtype=dtype):
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
            if JsonFlattenParser.is_struct(dtype=dtype):
                cols += JsonFlattenParser.flatten_struct_column(path=path, dtype=dtype)
            elif JsonFlattenParser.is_array(dtype=dtype):
                cols.append((path, "array"))
            elif JsonFlattenParser.is_map(dtype=dtype):
                cols.append((path, "map"))
            else:
                cols.append((path, dtype))
        cols_exprs: List[Tuple[str, str, str]] = []
        expr: str
        for path, dtype in cols:
            path_under = path.replace('.', '_').replace('-','_').replace(':','_').replace('/','_') \
                                   .replace('#','_') \
                                   .replace('"','_').replace('[','_').replace(']','_').replace(' ','_')
            if JsonFlattenParser.is_array(dtype):
                if explode_pos:
                    expr = f"{explode}({path}) AS ({path_under}_pos, {path_under})"
                else:
                    expr = f"{explode}({path}) AS {path_under}"
            elif JsonFlattenParser.is_map(dtype):
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
        exprs_arr: List[str] = [x[2] for x in cols_exprs if JsonFlattenParser.is_array_or_map(x[1])]
        print("exprs_arr is :",exprs_arr)
        exprs: List[str] = [x[2] for x in cols_exprs if not JsonFlattenParser.is_array_or_map(x[1])]
        print("exprs is :",exprs)
        if len(exprs)==0:
            empty_root_df_flag =True
        dfs: Dict[str, DataFrame] = {name: dataframe.selectExpr(exprs)}
        print("dfs is :",dfs)
        #return dfs
        exprs = [x[2] for x in cols_exprs if not JsonFlattenParser.is_array_or_map(x[1]) and not x[0].endswith("_pos")]
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

    def getflatteneddataframe(self,dictdfs:Dict[str, DataFrame])->DataFrame:
        '''
        :param dictdfs: dictionary of dataframes
        :return flattened datframe
        '''
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

    @staticmethod
    def flatten_level(df, sentinel="x"):
        '''
        This function will take a sparkdf with json records and parse it to level n
        :param df: df with json records
        :param sentinel: sentinel i.e. level to go deep
        :return flatteneddf to sentinel level
        '''
        def _gen_flatten_expr(schema, indent, parents, last, transform=False):
            def handle(field, last):
                path = parents + (field.name,)
                alias = (
                    " as "
                    + "_".join(path[1:] if transform else path)
                    + ("," if not last else "")
                )
                if isinstance(field.dataType, StructType):
                    yield from _gen_flatten_expr(
                        field.dataType, indent, path, last, transform
                    )
                elif (
                    isinstance(field.dataType, ArrayType) and
                    isinstance(field.dataType.elementType, StructType)
                ):
                    yield indent, "transform("
                    yield indent + 1, ".".join(path) + ","
                    yield indent + 1, sentinel + " -> struct("
                    yield from _gen_flatten_expr(
                        field.dataType.elementType, 
                        indent + 2, 
                        (sentinel,), 
                        True, 
                        True
                    )
                    yield indent + 1, ")"
                    yield indent, ")" + alias
                else:
                    yield (indent, ".".join(path) + alias)

            try:
                *fields, last_field = schema.fields
            except ValueError:
                pass
            else:
                for field in fields:
                    yield from handle(field, False)
                yield from handle(last_field, last)

        lines = []
        for indent, line in _gen_flatten_expr(df.schema, 0, (), True):
            spaces = " " * 4 * indent
            lines.append(spaces + line)

        expr = "struct(" + "\n".join(lines) + ") as " + sentinel
        return df.selectExpr(expr).select(sentinel + ".*")

    
    @staticmethod
    def explode_df(df):
        '''
        This function explode the df for each value in an array
        :param df: df with json records as array element
        :return exploded df
        '''
        from pyspark.sql.functions import explode_outer
        for c in df.dtypes:
            if c[1][:5]=='array':
               column_name = c[0]
               #print(column_name)
               df = df.select("*", explode_outer(df[column_name])).drop(column_name).withColumnRenamed("col",column_name) 
               #df.printSchema()
               #print(df.count())
        return df       

    @staticmethod
    def recur_column_name_fix(schema: StructType):
        '''
        Spark cannot handle these special charaters in column_name '[ ,;{}()\\n\\t=:\.\/#-&]' This wil lreplace them with '_'
        :param schema: schema of the json dataframe
        :return new schema with replaced special charaters
        '''
        import pyspark.sql.types as sql_types
        import re
        schema_new = []
        re_match = '[& ,;{}()\\n\\t=:\.\/#-]'
        for struct_field in schema:
            if type(struct_field.dataType)==sql_types.StructType:
                schema_new.append(sql_types.StructField(re.sub(re_match,'_',struct_field.name), sql_types.StructType(JsonFlattenParser.recur_column_name_fix(struct_field.dataType)), struct_field.nullable, struct_field.metadata))
            elif type(struct_field.dataType)==sql_types.ArrayType: 
                if type(struct_field.dataType.elementType)==sql_types.StructType:
                    schema_new.append(sql_types.StructField(re.sub(re_match,'_',struct_field.name), sql_types.ArrayType(sql_types.StructType(JsonFlattenParser.recur_column_name_fix(struct_field.dataType.elementType)),True), struct_field.nullable, struct_field.metadata)) # Recursive call to loop over all Array elements
                else:
                    schema_new.append(sql_types.StructField(re.sub(re_match,'_',struct_field.name), struct_field.dataType, struct_field.nullable, struct_field.metadata)) # If ArrayType only has one field, it is no sense to use an Array so Array is exploded
            else:
                schema_new.append(sql_types.StructField(re.sub(re_match,'_',struct_field.name), struct_field.dataType, struct_field.nullable, struct_field.metadata))
        return schema_new

    def flatten_df(self, df, loop_count = 1):
        '''
        Recursive function to flatten the df with json records at level n
        :param df:  dataframe with json records
        :param loop_count: level to go to
        :return: Flattened dataframe
        '''
        ret = df
        count = 0
        while count < loop_count:
            ret = JsonFlattenParser.flatten_level(ret)
            #ret.printSchema()
            #ret.show()
            ret = JsonFlattenParser.explode_df(ret)
            #ret.printSchema()
            #ret.show()
            count = count + 1 
        return ret


    def clean_df(self, spark, df):
        '''
        Spark cannot handle these special charaters in column_name '[ ,;{}()\\n\\t=:\.\/#-]' This wil lreplace them with '_'
        :param df : json dataframe
        :return df with replaced column names
        '''
        new_schema = JsonFlattenParser.recur_column_name_fix(df.schema)
        df2 = spark.createDataFrame(df.rdd, schema=types.StructType(new_schema))
        return df2
