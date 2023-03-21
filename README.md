+ desc

    The tool can help you parse select sql statement and extract column and table from origin sql.

+ use
    
    you can use sqlcolumnlineage like belowing :

    java -jar sqlcolumnlineage.jar -s "select id from A"

    you will get result : {A=[id]}

    or more complex sql:

    java -jar sqlcolumnlineage.jar -s "select a.id, b.code from A a, (select * from B) b"

    you will get result : {A=[id], B=[code]}

