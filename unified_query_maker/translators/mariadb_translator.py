from .mysql_translator import MySQLTranslator


class MariaDBTranslator(MySQLTranslator):
    """MariaDB specific translator - reuses MySQL implementation as they're very similar"""

    pass
