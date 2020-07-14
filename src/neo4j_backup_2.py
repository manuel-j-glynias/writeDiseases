from neo4j import GraphDatabase
import csv
import datetime

def send_to_neo4j(driver, payload):
    with driver.session() as session:
        tx = session.begin_transaction()
        result = tx.run(payload)
        # print(result)
        tx.commit()

def get_elapsed_time(now, start):
    old_now = now
    now = datetime.datetime.now()
    last_round = now - old_now
    elapsed = now - start
    return elapsed, last_round, now

def main():
    uri = "bolt://localhost:7687"

    with open('schema.graphql', 'r') as file:
        idl_as_string = file.read()
    driver = GraphDatabase.driver(uri, auth=("neo4j", "omni"))
    send_to_neo4j(driver,"match(a) detach delete(a)")
    send_to_neo4j(driver,"call graphql.idl('" + idl_as_string + "')")

    start = datetime.datetime.now()
    now = start

    send_to_neo4j(driver, 'CREATE INDEX ON :User(id)')
    read_users = '''LOAD CSV WITH HEADERS FROM 'file:///User.csv' AS row
    WITH row.name as name, row.password as password, toBoolean(row.isAdmin) as isAdmin, row.graph_id as id
    CREATE (:User {name:name, password:password, isAdmin:isAdmin, id:id})'''
    send_to_neo4j(driver, read_users)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("User", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :Author(id)')
    read_author = '''LOAD CSV WITH HEADERS FROM 'file:///Author.csv' AS row
        WITH row.surname as surname, row.firstInitial as firstInitial,  row.graph_id as id
        CREATE (:Author {surname:surname, firstInitial:firstInitial,  id:id})'''
    send_to_neo4j(driver, read_author)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("Author", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :Journal(id)')
    read_journal = '''LOAD CSV WITH HEADERS FROM 'file:///Journal.csv' AS row
            WITH row.name as name, row.graph_id as id
            CREATE (:Journal {name:name, id:id})'''
    send_to_neo4j(driver, read_journal)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("Journal", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver,'CREATE INDEX ON :LiteratureReference(id)')
    read_literature_reference = '''LOAD CSV WITH HEADERS FROM 'file:///LiteratureReference.csv' AS row
    WITH row.PMID as PMID,row.DOI as DOI, row.title as title, row.Journal_graph_id as j_id, row.volume as volume, row.firstPage as firstPage, row.lastPage as lastPage,row.publicationYear as publicationYear, row.shortReference as shortReference, row.abstract as abstract, row.graph_id as id
    MATCH(j:Journal) WHERE j.id=j_id
    CREATE (lr:LiteratureReference {PMID:PMID, DOI:DOI, title:title, volume:volume, firstPage:firstPage, lastPage:lastPage, publicationYear:publicationYear, shortReference:shortReference, abstract:abstract,id:id})
    CREATE (lr)-[:PUBLISHED_IN]->(j)'''
    send_to_neo4j(driver, read_literature_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("LiteratureReference", elapsed.total_seconds(), last_round.total_seconds())


    read_literature_reference_author = '''LOAD CSV WITH HEADERS FROM 'file:///LiteratureReference_Author.csv' AS row
    WITH row.Author_graph_id as author,row.LiteratureReference_graph_id as ref
    MATCH(lr:LiteratureReference) WHERE lr.id=ref
    MATCH(a:Author) WHERE a.id=author
    CREATE (lr)-[:AUTHORED_BY]->(a)'''
    send_to_neo4j(driver, read_literature_reference_author)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("LiteratureReference_Author", elapsed.total_seconds(), last_round.total_seconds())


    send_to_neo4j(driver,'CREATE INDEX ON :EditableStatement(id)')
    read_editable_statement = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStatement.csv' AS row
    WITH row.field as field, row.statement as statement,  row.editDate as editDate,row.editorId as editorId, row.graph_id as id
    MATCH(u:User) WHERE u.id=editorId
    CREATE (es:EditableStatement {field:field, statement:statement, editDate:editDate, id:id})
    CREATE (es)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_statement)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStatement", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_statement_literature_reference = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStatement_LiteratureReference.csv' AS row
    WITH row.EditableStatement_graph_id as editable,row.LiteratureReference_graph_id as ref
    MATCH(lr:LiteratureReference) WHERE lr.id=ref
    MATCH(es:EditableStatement) WHERE es.id=editable
    CREATE (es)-[:REFERENCE_FOR]->(lr)'''
    send_to_neo4j(driver, read_editable_statement_literature_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStatement_LiteratureReference", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :JaxDisease(id)')
    read_jax_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///jax_diseases.csv' AS row
    WITH row.jaxId as jaxId, row.name as name, row.source as source, row.definition as definition, row.termId as termId, row.graph_id as id
    MATCH(esd:EditableStatement) WHERE esd.id=definition
    MATCH(esn:EditableStatement) WHERE esn.id=name
    CREATE (jd:JaxDisease {jaxId:jaxId, source:source, termId:termId,  id:id})
    CREATE(jd) - [:DESCRIBED_BY]->(esd) 
    CREATE(jd) - [:NAMED]->(esn) '''
    send_to_neo4j(driver, read_jax_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("JaxDisease", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableStringList(id)')
    read_editable_string_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStringList.csv' AS row
     WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.graph_id as id 
     MATCH(u:User) WHERE u.id=editor
     CREATE (esl:EditableStringList :EditableObject {field:field, editDate:editDate,  id:id}) 
     CREATE (esl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_string_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStringList", elapsed.total_seconds(), last_round.total_seconds())

    read_EditableStringListElements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStringListElements.csv' AS row
        WITH row.EditableStringsList_graph_id as editable, split(row.stringList, "|") AS stringList
        MATCH(esyn:EditableStringList) WHERE esyn.id=editable
        SET esyn.stringList = stringList'''
    send_to_neo4j(driver, read_EditableStringListElements)
    print("converted_EditableStringListElements", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :XRef(id)')
    read_xref = '''LOAD CSV WITH HEADERS FROM 'file:///Xref.csv' AS row
          WITH row.graph_id as id, row.source as source, row.source_id  as sourceId 
          CREATE (xref:XRef {id:id, source:source, sourceId:sourceId }) '''
    send_to_neo4j(driver, read_xref)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("XRef", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableXRefList(id)')
    read_editable_xref_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableXrefsList.csv' AS row
         WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.xref_id as id 
         MATCH(u:User) WHERE u.id=editor
         CREATE (exl:EditableXRefList :EditableObject {field:field, editDate:editDate,  id:id}) 
         CREATE (exl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_xref_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableXRefList", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_xref_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableXrefsListElements.csv' AS row
            WITH row.EditableXRefList_graph_id as editable, split(row.XRef_graph_id, "|") AS XRef_graph_id
            MATCH(exrefsl:EditableXRefList) WHERE exrefsl.id=editable 
            SET exrefsl.list = XRef_graph_id '''
    send_to_neo4j(driver, read_editable_xref_list_elements)
    print("EditableXrefsListElements", elapsed.total_seconds(), last_round.total_seconds())

    read_xref = ''' MATCH (exrefslist:EditableXRefList) UNWIND exrefslist.list as refs  MATCH (xref:XRef  {id:refs})  
    CREATE (exrefslist)-[:XREF]->(xref)  '''
    send_to_neo4j(driver, read_xref)

    send_to_neo4j(driver, 'CREATE INDEX ON :DODisease(id)')
    read_do_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///do_diseases.csv' AS row
        WITH row.doId as doId, row.name as name, row.definition  as definition, row.exact_synonyms as exactSynonyms, 
        row.related_synonyms as relatedSynonyms,  row.subsets as subsets, row.xrefs as xrefs, row.graph_id as id
        MATCH(esd:EditableStatement) WHERE esd.id=definition
        MATCH(esn:EditableStatement) WHERE esn.id=name
        MATCH(eslexact:EditableStringList) WHERE eslexact.id=exactSynonyms
        MATCH(eslrelated:EditableStringList) WHERE eslrelated.id=relatedSynonyms
        MATCH(eslsubsets:EditableStringList) WHERE eslsubsets.id=subsets 
        MATCH(xreflist:EditableXRefList) WHERE xreflist.id=xrefs
        CREATE (do:DODisease {doId:doId, id:id})
        CREATE(do) - [:DESCRIBED_BY]->(esd) 
        CREATE(do) - [:NAMED]->(esn)
        CREATE(do) - [:ALSO_NAMED_EXACTLY]->(eslexact) 
        CREATE(do) - [:ALSO_NAMED_RELATED]->(eslrelated)
        CREATE(do) - [:SUBSET]->(eslsubsets)
        CREATE(do) - [:XREF]->(xreflist)'''
    send_to_neo4j(driver, read_do_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("DODisease", elapsed.total_seconds(), last_round.total_seconds())

    read_do_disease_parents = '''LOAD CSV WITH HEADERS FROM 'file:///do_parents.csv' AS row
    WITH row.graph_id as id, split(row.parent, "|") AS parents
    MATCH(dodisease:DODisease) WHERE dodisease.id=id 
    SET dodisease.parents = parents'''
    send_to_neo4j(driver, read_do_disease_parents)
    print("do parents", elapsed.total_seconds(), last_round.total_seconds())

    read_do_disease_children = '''LOAD CSV WITH HEADERS FROM 'file:///do_children.csv' AS row
       WITH row.graph_id as id, split(row.child, "|") AS children
       MATCH(dodisease:DODisease) WHERE dodisease.id=id 
       SET dodisease.children = children'''
    send_to_neo4j(driver, read_do_disease_children)
    print("do children", elapsed.total_seconds(), last_round.total_seconds())


    connect_do_parents = ''' MATCH (dodisease:DODisease) UNWIND dodisease.parents as parent  MATCH (do:DODisease  {id:parent})  
       CREATE (dodisease)-[:PARENT]->(do)  '''
    send_to_neo4j(driver, connect_do_parents)
    print("connect_do_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_do_children = ''' MATCH (dodisease:DODisease) UNWIND dodisease.children as child  MATCH (do:DODisease  {id:child})  
          CREATE (dodisease)-[:CHILD]->(do)  '''
    send_to_neo4j(driver, connect_do_children)
    print("connect_do_children", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :GODisease(id)')
    read_go_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///go_diseases.csv' AS row
          WITH row.id as goId, row.name as name, row.definition  as definition, row.synonyms as synonyms,  
          row.xrefs as xrefs, row.graph_id as id
          MATCH(esd:EditableStatement) WHERE esd.id=definition
          MATCH(esn:EditableStatement) WHERE esn.id=name
          MATCH(synlist:EditableStringList) WHERE synlist.id=synonyms 
           MATCH(xreflist:EditableXRefList) WHERE xreflist.id=xrefs
          CREATE (go:GODisease {goId:goId, id:id})
          CREATE(go) - [:DESCRIBED_BY]->(esd) 
          CREATE(go) - [:NAMED]->(esn)
          CREATE(go) - [:ALSO_NAMED]->(synlist) 
          CREATE(go) - [:XREF]->(xreflist) '''
    send_to_neo4j(driver, read_go_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("GODisease", elapsed.total_seconds(), last_round.total_seconds())

    read_go_disease_parents = '''LOAD CSV WITH HEADERS FROM 'file:///go_parents.csv' AS row
       WITH row.graph_id as id, split(row.parent, "|") AS parents
       MATCH(godisease:GODisease) WHERE godisease.id=id 
       SET godisease.parents = parents'''
    send_to_neo4j(driver, read_go_disease_parents)
    print("go parents", elapsed.total_seconds(), last_round.total_seconds())

    read_go_disease_children = '''LOAD CSV WITH HEADERS FROM 'file:///go_children.csv' AS row
          WITH row.graph_id as id, split(row.child, "|") AS children
          MATCH(godisease:GODisease) WHERE godisease.id=id 
          SET godisease.children = children'''
    send_to_neo4j(driver, read_go_disease_children)
    print("do children", elapsed.total_seconds(), last_round.total_seconds())

    connect_go_parents = ''' MATCH (godisease:GODisease) UNWIND godisease.parents as parent  MATCH (go:GODisease  {id:parent})  
          CREATE (godisease)-[:PARENT]->(go)  '''
    send_to_neo4j(driver, connect_go_parents)
    print("connect_do_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_go_children = ''' MATCH (godisease:GODisease) UNWIND godisease.children as child  MATCH (go:GODisease  {id:child})  
             CREATE (godisease)-[:CHILD]->(go)  '''
    send_to_neo4j(driver, connect_go_children)
    print("connect_go_children", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :OncoTreeDisease(id)')
    read_oncotree_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///oncotree_diseases.csv' AS row
            WITH row.code as code, row.name as name, row.mainType  as mainType, row.tissue as tissue,  
            row.xrefs as xrefs, row.graph_id as id
            MATCH(esmaintype:EditableStatement) WHERE esmaintype.id=mainType
            MATCH(esn:EditableStatement) WHERE esn.id=name
            MATCH(estissue:EditableStatement) WHERE estissue.id=tissue
            MATCH(xreflist:EditableXRefList) WHERE xreflist.id=xrefs
            CREATE (ot:OncoTreeDisease {code:code, id:id})
            CREATE(ot) - [:MAIN_ONCOTREE_TYPE]->(esmaintype) 
            CREATE(ot) - [:NAMED]->(esn)
            CREATE(ot) - [:TISSUE]->(estissue) 
            CREATE(ot) - [:XREF]->(xreflist)'''
    send_to_neo4j(driver, read_oncotree_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("OncoTreeDisease", elapsed.total_seconds(), last_round.total_seconds())

    read_onco_disease_parents = '''LOAD CSV WITH HEADERS FROM 'file:///oncotree_parents.csv' AS row
          WITH row.graph_id as id, split(row.parent, "|") AS parents
          MATCH(oncodisease:OncoTreeDisease) WHERE oncodisease.id=id 
          SET oncodisease.parents = parents'''
    send_to_neo4j(driver, read_onco_disease_parents)
    print("onco  parents", elapsed.total_seconds(), last_round.total_seconds())

    read_onco_disease_children = '''LOAD CSV WITH HEADERS FROM 'file:///oncotree_children.csv' AS row
             WITH row.graph_id as id, split(row.child, "|") AS children
             MATCH(oncodisease:OncoTreeDisease) WHERE oncodisease.id=id 
             SET oncodisease.children = children'''
    send_to_neo4j(driver, read_onco_disease_children)
    print("do children", elapsed.total_seconds(), last_round.total_seconds())

    connect_onco_parents = ''' MATCH (oncodisease:OncoTreeDisease) UNWIND oncodisease.parents as parent  MATCH (od:OncoTreeDisease  {id:parent})  
             CREATE (oncodisease)-[:PARENT]->(od)  '''
    send_to_neo4j(driver, connect_onco_parents)
    print("connect_do_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_onco_children = ''' MATCH (oncodisease:OncoTreeDisease) UNWIND oncodisease.children as child  MATCH (od:OncoTreeDisease  {id:child})  
                CREATE (oncodisease)-[:CHILD]->(od)  '''
    send_to_neo4j(driver, connect_onco_children)
    print("connect_go_children", elapsed.total_seconds(), last_round.total_seconds())



    send_to_neo4j(driver, 'CREATE INDEX ON :OmniDisease(id)')
    read_omni_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///omni_diseases.csv' AS row
            WITH row.omniDiseaseId as omniDiseaseId, row.name as name, row.omniDiseaseType  as omniDiseaseType, row.tissue as tissue,  row.graph_id as id
            MATCH(esn:EditableStatement) WHERE esn.id=name
            CREATE (od:OmniDisease {omniDiseaseId:omniDiseaseId, omniDiseaseType:omniDiseaseType,  id:id})
            CREATE(od) - [:NAMED]->(esn)'''
    send_to_neo4j(driver, read_omni_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("OmniDisease", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :MCode(id)')
    read_mcode_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///mcode_diseases.csv' AS row
    WITH row.mcode as mcodeId, row.diseasePath as diseasePath, row.omniDisease  as omniDisease,  row.graph_id as id
    MATCH(esn:EditableStatement) WHERE esn.id=diseasePath
    CREATE (mc:MCode {mcodeId:mcodeId, omniDisease:omniDisease,  id:id})
    CREATE(mc) - [:DISEASE_PATH]->(esn)'''
    send_to_neo4j(driver, read_mcode_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("MCode", elapsed.total_seconds(), last_round.total_seconds())

    read_mcode_disease_parents = '''LOAD CSV WITH HEADERS FROM 'file:///mcode_parents.csv' AS row
            WITH row.graph_id as id, split(row.parent, "|") AS parents
            MATCH(mcode:MCode) WHERE mcode.id=id 
            SET mcode.parents = parents'''
    send_to_neo4j(driver, read_mcode_disease_parents)
    print("mcode  parents", elapsed.total_seconds(), last_round.total_seconds())

    read_mcode_disease_children = '''LOAD CSV WITH HEADERS FROM 'file:///mcode_children.csv' AS row
               WITH row.graph_id as id, split(row.child, "|") AS children
               MATCH(mcode:MCode) WHERE mcode.id=id 
               SET mcode.children = children'''
    send_to_neo4j(driver, read_mcode_disease_children)
    print("mcode children", elapsed.total_seconds(), last_round.total_seconds())

    connect_mcode_parents = ''' MATCH (mcode:MCode) UNWIND mcode.parents as parent  MATCH (mc:MCode  {id:parent})  
               CREATE (mcode)-[:PARENT]->(mc) '''
    send_to_neo4j(driver, connect_mcode_parents)
    print("connect_mcode_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_mcode_children = ''' MATCH (mcode:MCode) UNWIND mcode.children as child  MATCH (mc:MCode  {id:child})  
                  CREATE (mcode)-[:CHILD]->(mc) '''
    send_to_neo4j(driver, connect_mcode_children)
    print("connect_mcode_children", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :TCode(id)')
    read_tcode_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///tcode_diseases.csv' AS row
       WITH row.tcode as tcodeId, row.tissuePath as tissuePath, row.graph_id as id
       MATCH(esn:EditableStatement) WHERE esn.id=tissuePath
       CREATE (tc:TCode {tcodeId:tcodeId, id:id})
       CREATE(tc) - [:TISSUE_PATH]->(esn)'''
    send_to_neo4j(driver, read_tcode_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("TCode", elapsed.total_seconds(), last_round.total_seconds())


    read_tcode_disease_parents = '''LOAD CSV WITH HEADERS FROM 'file:///tcode_parents.csv' AS row
            WITH row.graph_id as id, split(row.parent, "|") AS parents
            MATCH(tcode:TCode) WHERE tcode.id=id 
            SET tcode.parents = parents'''
    send_to_neo4j(driver, read_tcode_disease_parents)
    print("tcode  parents", elapsed.total_seconds(), last_round.total_seconds())

    read_tcode_disease_children = '''LOAD CSV WITH HEADERS FROM 'file:///tcode_children.csv' AS row
               WITH row.graph_id as id, split(row.child, "|") AS children
               MATCH(tcode:TCode) WHERE tcode.id=id 
               SET tcode.children = children'''
    send_to_neo4j(driver, read_tcode_disease_children)
    print("tcode children", elapsed.total_seconds(), last_round.total_seconds())

    connect_tcode_parents = ''' MATCH (tcode:TCode) UNWIND tcode.parents as parent  MATCH (tc:TCode  {id:parent})  
               CREATE (tcode)-[:PARENT]->(tc) '''
    send_to_neo4j(driver, connect_tcode_parents)
    print("connect_tcode_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_tcode_children = ''' MATCH (tcode:TCode) UNWIND tcode.children as child  MATCH (tc:TCode  {id:child})  
                  CREATE (tcode)-[:CHILD]->(tc) '''
    send_to_neo4j(driver, connect_tcode_children)
    print("connect_tcode_children", elapsed.total_seconds(), last_round.total_seconds())

    driver.close()

if __name__ == "__main__":
    main()

