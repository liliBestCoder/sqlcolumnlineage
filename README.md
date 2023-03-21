+ desc

    The tool can help you parse sql select statement and extract column and table from the original sql.

+ use
    
    you can use sqlcolumnlineage like belowing :

    java -jar sqlcolumnlineage.jar -s "select id from A"

    you will get result : {A=[id]}

    or more complex sql:

    java -jar sqlcolumnlineage.jar -s "select a.id, b.code from A a, (select * from B) b"

    you will get result : {A=[id], B=[code]}

