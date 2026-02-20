import ast
import polars as pl
import re

class FilterParser:
    def __init__(self):
        self.OPERATORS = {
            ' gt ' : ' > ',
            ' lt ' : ' < ',
            ' gte ': ' >= ',
            ' lte ': ' <= ',
            ' eq ': ' == ',
            ' neq ': ' != ',
        }

    def create_plr_expr(self, expr):
        if isinstance(expr, ast.Compare):
            left = self.create_plr_expr(expr.left)
            op = expr.ops[0]
            
            right = self.create_plr_expr(expr.comparators[0])

            if isinstance(op, ast.Gt): return left > right
            if isinstance(op, ast.Lt): return left < right
            if isinstance(op, ast.GtE): return left >= right
            if isinstance(op, ast.LtE): return left <= right
            if isinstance(op, ast.Eq): return left == right
            if isinstance(op, ast.NotEq): return left != right

        elif isinstance(expr, ast.BinOp):
            left = self.create_plr_expr(expr.left)
            right = self.create_plr_expr(expr.right)
            if isinstance(expr.op, ast.BitAnd): return left & right
            if isinstance(expr.op, ast.BitOr): return left | right

        elif isinstance(expr, ast.Name):
            return pl.col(expr.id)

        elif isinstance(expr, ast.Constant):
            return pl.lit(expr.value)

        elif isinstance(expr, ast.Expression):
            return self.create_plr_expr(expr.body)

        raise ValueError(f"Invalid operation: {type(expr)}")

    def map_operators(self, query_str):
        for word, symbol in self.OPERATORS.items():
            query_str = query_str.replace(word, symbol)
        return query_str

    def parse(self,query_str):
        query_str = self.map_operators(query_str)
        query_str = re.sub(
            r'(==|!=)\s*([A-Za-z][A-Za-z0-9_]*)',
            lambda m: f'{m.group(1)} \'{m.group(2)}\'',
            query_str
        )

        if '(' not in query_str:
            query = re.sub(r'(\w+\s*(?:==|!=|>=|<=|>|<)\s*(?:\'[^\']*?\'|"[^"]*?"|\d+\.\d+|\d+|\w+))', r'(\1)', query_str)
        else:
            query = query_str
        query = query.replace(" and ", " & ").replace(" or ", " | ")
        tree = ast.parse(query, mode='eval')
        return self.create_plr_expr(tree)

