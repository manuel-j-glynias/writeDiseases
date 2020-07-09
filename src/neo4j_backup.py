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
    send_to_neo4j(driver, "match(a) detach delete(a)")
    send_to_neo4j(driver, "call graphql.idl('" + idl_as_string + "')")

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

    send_to_neo4j(driver, 'CREATE INDEX ON :LiteratureReference(id)')
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

    """
    send_to_neo4j(driver,'CREATE INDEX ON :InternetReference(id)')
    read_internet_reference = '''LOAD CSV WITH HEADERS FROM 'file:///InternetReference.csv' AS row
    WITH row.accessedDate as accessedDate, row.webAddress as webAddress,  row.shortReference as shortReference, row.graph_id as id
    CREATE (:InternetReference {accessedDate:accessedDate, webAddress:webAddress, shortReference:shortReference, id:id})'''
    send_to_neo4j(driver, read_internet_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("InternetReference", elapsed.total_seconds(), last_round.total_seconds())
    """
    send_to_neo4j(driver, 'CREATE INDEX ON :EditableStatement(id)')
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
     CREATE (esl:EditableStringList {field:field, editDate:editDate,  id:id}) 
     CREATE (esl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_string_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStringList", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableStringListElement(id)')
    read_editable_string_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStringListElements.csv' AS row
    WITH row.id as id, row.name as name, row.EditableStringsList_graph_id as editableStringListId
    MATCH(eslist:EditableStringList) WHERE eslist.id=editableStringListId
    CREATE (eslel:EditableStringListElement {id:id, name:name, editableStringListId:editableStringListId}) 
    CREATE (eslel)-[:ELEMENT_OF]->(eslist)'''
    send_to_neo4j(driver, read_editable_string_list_elements)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStringListElement", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableXRefList(id)')
    read_editable_xref_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableXrefsList.csv' AS row
     WITH row.graph_id  as field, row.edit_date as editDate, row.editor_id as editor, row.xref_id as id  
     MATCH(u:User) WHERE u.id=editor
     CREATE (exl:EditableXRefList {field:field, editDate:editDate,  id:id}) 
     CREATE (exl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_xref_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableXRefList", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :XRef(id)')
    read_xref = '''LOAD CSV WITH HEADERS FROM 'file:///Xref.csv' AS row
    WITH row.graph_id as id, row.source as source, row.source_id  as sourceId 
    CREATE (xref:XRef {id:id, source:source, sourceId:sourceId }) '''
    send_to_neo4j(driver, read_xref)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("XRef", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableXrefListElement(id)')
    read_editable_xref_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableXrefsListElements.csv' AS row
    WITH row.id as id, row.XRef_graph_id as xRef, row.EditableXRefList_graph_id as editableXrefList
    MATCH(exlst:EditableXRefList) WHERE exlst.id=editableXrefList
    MATCH(ref:XRef) WHERE ref.id=xRef
    CREATE (exlel:EditableXrefListElement {id:id}) 
    CREATE (exlel)-[:ELEMENT_OF]->(exlst)
     CREATE (exlel)-[:XREF]->(ref)'''
    send_to_neo4j(driver, read_editable_xref_list_elements)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableXrefListElement", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableJAXDiseaseList(id)')
    read_editable_jax_disease_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableJaxDiseaseList.csv' AS row
      WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.EditableDiseaseList_graph_id as id 
      MATCH(u:User) WHERE u.id=editor
      CREATE (ejdl:EditableJAXDiseaseList {field:field, editDate:editDate,  id:id}) 
      CREATE (ejdl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_jax_disease_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableJaxDiseaseList", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableJAXDiseaseListElement(id)')
    read_editable_jax_disease_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableJAXDiseaseListElements.csv' AS row
    WITH row.id as id, row.Disease_graph_id as jaxDisease, row.EditableDiseaseList_graph_id  as editableJAXDiseaseList
    MATCH(ejdl:EditableJAXDiseaseList) WHERE ejdl.id=editableJAXDiseaseList
    MATCH(jd:JaxDisease) WHERE jd.id=jaxDisease
    CREATE (ejdle:EditableJAXDiseaseListElement {id:id}) 
    CREATE (ejdle)-[:ELEMENT_OF]->(ejdl)
    CREATE (ejdle)-[:DISEASE]->(jd)'''
    send_to_neo4j(driver, read_editable_jax_disease_list_elements)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableJAXDiseaseListElement", elapsed.total_seconds(), last_round.total_seconds())

    """
    send_to_neo4j(driver, 'CREATE INDEX ON :XRef(id)')
    read_xref = '''LOAD CSV WITH HEADERS FROM 'file:///Xref.csv' AS row
   WITH row.graph_id as xrefId, row.source as source, row.source_id  as sourceId 
   MATCH(exlelement:EditableXrefListElement) WHERE exlelement.xRefGraphId=xrefId
   CREATE (xref:XRef {source:source, sourceId:sourceId }) 
   CREATE (xref)-[:ELEMENT_OF]->(exlelement)'''
    send_to_neo4j(driver, read_xref)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("XRef", elapsed.total_seconds(), last_round.total_seconds())




    read_editable_statement_internet_reference = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStatement_InternetReference.csv' AS row
    WITH row.EditableStatement_graph_id as editable,row.InternetReference_graph_id as ref
    MATCH(ir:InternetReference) WHERE ir.id=ref
    MATCH(es:EditableStatement) WHERE es.id=editable
    CREATE (es)-[:REFERENCE_FOR]->(ir)'''
    send_to_neo4j(driver, read_editable_statement_internet_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStatement_InternetReference", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver,'CREATE INDEX ON :EditableSynonymList(id)')
    read_editable_synonym_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableSynonymList.csv' AS row
    WITH row.field as field, row.editDate as editDate,row.editorId as editorId, row.graph_id as id
    MATCH(u:User) WHERE u.id=editorId
    CREATE (esyn:EditableSynonymList {field:field, list:[], editDate:editDate, id:id})
    CREATE (esyn)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_synonym_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableSynonymList", elapsed.total_seconds(), last_round.total_seconds())

    # read_synonyms = '''LOAD CSV FROM 'file:///converted_Synonym.csv AS row
    # WITH row[0] as editable, split(row[1], ',') AS synonyms
    # MATCH(esyn:EditableSynonymList) WHERE esyn.id=editable
    # MERGE
    # '''


    read_editable_synonym_literature_reference = '''LOAD CSV WITH HEADERS FROM 'file:///EditableSynonymList_LiteratureReference.csv' AS row
    WITH row.EditableSynonymList_graph_id as editable,row.LiteratureReference_graph_id as ref
    MATCH(lr:LiteratureReference) WHERE lr.id=ref
    MATCH(esyn:EditableSynonymList) WHERE esyn.id=editable
    CREATE (esyn)-[:REFERENCE_FOR]->(lr)'''
    send_to_neo4j(driver, read_editable_synonym_literature_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableSynonymList_LiteratureReference", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_statement_internet_reference = '''LOAD CSV WITH HEADERS FROM 'file:///EditableSynonymList_InternetReference.csv' AS row
    WITH row.EditableSynonymList_graph_id as editable,row.InternetReference_graph_id as ref
    MATCH(ir:InternetReference) WHERE ir.id=ref
    MATCH(esyn:EditableSynonymList) WHERE esyn.id=editable
    CREATE (esyn)-[:REFERENCE_FOR]->(ir)'''
    send_to_neo4j(driver, read_editable_statement_internet_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableSynonymList_InternetReference", elapsed.total_seconds(), last_round.total_seconds())

    """
    driver.close()


if __name__ == "__main__":
    main()

