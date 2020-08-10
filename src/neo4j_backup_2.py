from neo4j import GraphDatabase
import csv
import datetime
import time

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


def write_to_prod():
    return '165.227.89.140'

def main():
    uri = 'bolt://' + write_to_prod() + ':7687'

    #with open('schema_03_10.graphql', 'r') as file:
        #idl_as_string = file.read()
    driver = GraphDatabase.driver(uri, auth=("neo4j", "omni"))
    #send_to_neo4j(driver, "match(a) detach delete(a)")
    #send_to_neo4j(driver, "call graphql.idl('" + idl_as_string + "')")

    start = datetime.datetime.now()
    now = start


    #send_to_neo4j(driver, 'CREATE INDEX ON :Author(id)')
    read_author = '''LOAD CSV WITH HEADERS FROM 'file:///Author_ik.csv' AS row
                   WITH row.surname as surname, row.firstInitial as firstInitial,  row.graph_id as id
                   CREATE (:Author {surname:surname, firstInitial:firstInitial,  id:id})'''
    send_to_neo4j(driver, read_author)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("Author", elapsed.total_seconds(), last_round.total_seconds())

    #send_to_neo4j(driver, 'CREATE INDEX ON :Journal(id)')
    read_journal = '''LOAD CSV WITH HEADERS FROM 'file:///Journal_ik.csv' AS row
                       WITH row.name as name, row.graph_id as id
                       CREATE (:Journal {name:name, id:id})'''
    send_to_neo4j(driver, read_journal)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("Journal", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 15  seconds')
    time.sleep(15)
    print('Start')

    #send_to_neo4j(driver,'CREATE INDEX ON :LiteratureReference(id)')
    read_literature_reference = '''LOAD CSV WITH HEADERS FROM 'file:///LiteratureReference_ik.csv' AS row
               WITH row.PMID as PMID,row.DOI as DOI, row.title as title, row.Journal_graph_id as j_id, row.volume as volume, row.firstPage as firstPage, row.lastPage as lastPage,row.publicationYear as publicationYear, row.shortReference as shortReference, row.abstract as abstract, row.graph_id as id
               MATCH(j:Journal) WHERE j.id=j_id
               CREATE (lr:LiteratureReference {PMID:PMID, DOI:DOI, title:title, volume:volume, firstPage:firstPage, lastPage:lastPage, publicationYear:publicationYear, shortReference:shortReference, abstract:abstract,id:id})
               CREATE (lr)-[:PUBLISHED_IN]->(j)'''
    send_to_neo4j(driver, read_literature_reference)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("LiteratureReference", elapsed.total_seconds(), last_round.total_seconds())

    read_literature_reference_author = '''LOAD CSV WITH HEADERS FROM 'file:///LiteratureReference_Author_ik.csv' AS row
               WITH row.Author_graph_id as author,row.LiteratureReference_graph_id as ref
               MATCH(lr:LiteratureReference) WHERE lr.id=ref
               MATCH(a:Author) WHERE a.id=author
               CREATE (lr)-[:AUTHORED_BY]->(a)'''
    send_to_neo4j(driver, read_literature_reference_author)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("LiteratureReference_Author", elapsed.total_seconds(), last_round.total_seconds())

    #send_to_neo4j(driver,'CREATE INDEX ON :EditableStatement(id)')
    read_editable_statement = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStatement_ik.csv' AS row
               WITH row.field as field, row.statement as statement,  row.editDate as editDate,row.editorId as editorId, row.graph_id as id
               MATCH(u:User) WHERE u.id=editorId
               CREATE (es:EditableStatement {field:field, statement:statement, editDate:editDate, id:id})
               CREATE (es)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_statement)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStatement", elapsed.total_seconds(), last_round.total_seconds())

    #send_to_neo4j(driver, 'CREATE INDEX ON :EditableBoolean(id)')
    read_editable_boolean_statement = '''LOAD CSV WITH HEADERS FROM 'file:///EditableBoolean_ik.csv' AS row
                 WITH row.field as field, row.booleanValue as booleanValue,  row.editDate as editDate,row.editorId as editorId, row.graph_id as id
                 MATCH(u:User) WHERE u.id=editorId
                 CREATE (es:EditableBoolean {field:field, booleanValue:booleanValue, editDate:editDate, id:id})
                 CREATE (es)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_boolean_statement)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableBoolean", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 15 seconds')
    time.sleep(15)
    print('Start')

    read_editable_statement_literature_reference = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStatement_LiteratureReference_ik.csv' AS row
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

    # send_to_neo4j(driver, 'CREATE INDEX ON :EditableStringList(id)')
    read_editable_string_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStringList_ik.csv' AS row
                WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.graph_id as id 
                MATCH(u:User) WHERE u.id=editor
                CREATE (esl:EditableStringList :EditableObject {field:field, editDate:editDate,  id:id}) 
                CREATE (esl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_string_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableStringList", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 15 seconds')
    time.sleep(15)
    print('Start')

    read_EditableStringListElements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableStringListElements_ik.csv' AS row
                   WITH row.EditableStringsList_graph_id as editable, split(row.stringList, "|") AS stringList
                   MATCH(esyn:EditableStringList) WHERE esyn.id=editable
                   SET esyn.stringList = stringList'''
    send_to_neo4j(driver, read_EditableStringListElements)
    print("converted_EditableStringListElements", elapsed.total_seconds(), last_round.total_seconds())

    ###############################################################################################
    send_to_neo4j(driver, 'CREATE INDEX ON :XRef(id)')
    read_xref = '''LOAD CSV WITH HEADERS FROM 'file:///Xref_ik.csv' AS row
                     WITH row.graph_id as id, row.source as source, row.source_id  as sourceId 
                     CREATE (xref:XRef {id:id, source:source, sourceId:sourceId }) '''
    send_to_neo4j(driver, read_xref)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("XRef", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableXRefList(id)')
    read_editable_xref_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableXrefsList_ik.csv' AS row
                    WITH row.graph_id as field, row.edit_date as editDate, row.editor_id as editor, row.xref_id as id 
                    MATCH(u:User) WHERE u.id=editor
                    CREATE (exl:EditableXRefList :EditableObject {field:field, editDate:editDate,  id:id}) 
                    CREATE (exl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_xref_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableXRefList", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 30 seconds')
    time.sleep(30)
    print('Start')

    read_editable_xref_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableXrefsListElements_ik.csv' AS row
                       WITH row.EditableXRefList_graph_id as editable, split(row.XRef_graph_id, "|") AS XRef_graph_id
                       MATCH(exrefsl:EditableXRefList) WHERE exrefsl.id=editable 
                       SET exrefsl.list = XRef_graph_id '''
    send_to_neo4j(driver, read_editable_xref_list_elements)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableXrefsListElements", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 60 seconds')
    time.sleep(60)
    print('Start')

    read_xref = ''' MATCH (exrefslist:EditableXRefList) UNWIND exrefslist.list as refs  MATCH (xref:XRef  {id:refs})  
               CREATE (exrefslist)-[:XREF]->(xref)  '''
    send_to_neo4j(driver, read_xref)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("Connect xrefs", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 60 seconds')
    time.sleep(60)
    print('Start')

    send_to_neo4j(driver, 'CREATE INDEX ON :DODisease(id)')
    read_do_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///do_diseases.csv' AS row
                      WITH row.doId as doId, row.name as name, row.definition  as definition, row.exact_synonyms as exactSynonyms, 
                      row.related_synonyms as relatedSynonyms,  row.subset as subsets, row.xrefs as xrefs, row.graph_id as id
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

    print('Stop 30 seconds')
    time.sleep(30)
    print('Start')

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

    print('Stop 15 seconds')
    time.sleep(15)
    print('Start')

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

    print('Stop 15 seconds')
    time.sleep(15)
    print('Start')

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
    print("onco children", elapsed.total_seconds(), last_round.total_seconds())

    connect_onco_parents = ''' MATCH (oncodisease:OncoTreeDisease) UNWIND oncodisease.parents as parent  MATCH (od:OncoTreeDisease  {id:parent})  
                           CREATE (oncodisease)-[:PARENT]->(od)  '''
    send_to_neo4j(driver, connect_onco_parents)
    print("connect_onco_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_onco_children = ''' MATCH (oncodisease:OncoTreeDisease) UNWIND oncodisease.children as child  MATCH (od:OncoTreeDisease  {id:child})  
                              CREATE (oncodisease)-[:CHILD]->(od)  '''
    send_to_neo4j(driver, connect_onco_children)
    print("connect_onco_children", elapsed.total_seconds(), last_round.total_seconds())

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
                  WITH row.mcode as mcodeId, row.diseasePath as diseasePath, row.omniDisease  as omniDisease,  row.active as active,  row.graph_id as id
                  MATCH(esn:EditableStatement) WHERE esn.id=diseasePath
                  MATCH(eb:EditableBoolean) WHERE eb.id=active
                  MATCH(od:OmniDisease) WHERE od.id=omniDisease
                  CREATE (mc:MCode {mcodeId:mcodeId, id:id})
                  CREATE(mc) - [:DISEASE_PATH]->(esn)
                  CREATE(mc) - [:OMNIDISEASE]->(od)
                  CREATE(mc) - [:ACTIVE]->(eb)'''
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

    ###########################################################################################################

    print('Stop 15 seconds')
    time.sleep(15)
    print('Start')

    send_to_neo4j(driver, 'CREATE INDEX ON :OmniMap(id)')
    read_omnimap = '''LOAD CSV WITH HEADERS FROM 'file:///Omni_map.csv' AS row
                               WITH row.graph_id as id, row.omniDisease_graph_id as omniDisease, row.mCode_graph_id  as mCodes 
                               MATCH(omnidisease:OmniDisease) WHERE omnidisease.id=omniDisease
                               MATCH(mcode:MCode) WHERE mcode.id=mCodes
                               CREATE (omnimap:OmniMap {id:id }) 
                               CREATE (omnimap)-[:OMNIDISEASE]->(omnidisease)
                               CREATE (omnimap)-[:MCODE]->(mcode)'''
    send_to_neo4j(driver, read_omnimap)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("OmniMap", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableOmniMapList(id)')
    read_editable_omnimap_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableOmniMapList.csv' AS row
                              WITH row.graph_id as field, row.edit_date as editDate, row.editor_id as editor, row.xref_id as id 
                              MATCH(u:User) WHERE u.id=editor
                              CREATE (eomll:EditableOmniMapList :EditableObject {field:field, editDate:editDate,  id:id}) 
                              CREATE (eomll)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_omnimap_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableOmniMapList(", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_omnimap_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableOmniMapListElements.csv' AS row
                                 WITH row.EditableMapList_graph_id as editable, split(row.OmniMap_graph_id, "|") AS OmniMap_graph_id
                                 MATCH(oml:EditableOmniMapList) WHERE oml.id=editable 
                                 SET oml.list = OmniMap_graph_id '''
    send_to_neo4j(driver, read_editable_omnimap_list_elements)
    print("EditableOmniMapList(Elements", elapsed.total_seconds(), last_round.total_seconds())

    read_omni = ''' MATCH (oml:EditableOmniMapList) UNWIND oml.list as omnimaps  MATCH (omni:OmniMap  {id:omnimaps})  
                         CREATE (oml)-[:OMNIMAP]->(omni)  '''
    send_to_neo4j(driver, read_omni)
    print("Connect omni", elapsed.total_seconds(), last_round.total_seconds())

    #######################################################################################################################

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

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableGODiseaseList(id)')
    read_editable_go_disease_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableGoDiseaseList.csv' AS row
                         WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.EditableDiseaseList_graph_id as id
                         MATCH(u:User) WHERE u.id=editor
                         CREATE (egdl:EditableGODiseaseList :EditableObject {field:field, editDate:editDate,  id:id}) 
                         CREATE (egdl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_go_disease_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableGODiseaseList", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_go_disease_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableGoDiseaseListElements.csv' AS row
                            WITH row.EditableDiseaseList_graph_id as editable, split(row.Disease_graph_id, "|") AS Disease_graph_id
                            MATCH(egdl:EditableGODiseaseList) WHERE egdl.id=editable 
                            SET egdl.list = Disease_graph_id '''
    send_to_neo4j(driver, read_editable_go_disease_list_elements)
    print("EditableGoDiseaseListElements", elapsed.total_seconds(), last_round.total_seconds())

    read_go_to_go_disease = ''' MATCH (egdl:EditableGODiseaseList) UNWIND egdl.list as go_diseases  MATCH (go:GODisease  {id:go_diseases})  
                    CREATE (egdl)-[:GODISEASE]->(go)  '''
    send_to_neo4j(driver, read_go_to_go_disease)
    print("Match editable  go disease", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableDODiseaseList(id)')
    read_editable_do_disease_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableDoDiseaseList.csv' AS row
                         WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.EditableDiseaseList_graph_id as id
                         MATCH(u:User) WHERE u.id=editor
                         CREATE (eddl:EditableDODiseaseList :EditableObject {field:field, editDate:editDate,  id:id}) 
                         CREATE (eddl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_do_disease_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableDODiseaseList", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_do_disease_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableDoDiseaseListElements.csv' AS row
                            WITH row.EditableDiseaseList_graph_id as editable, split(row.Disease_graph_id, "|") AS Disease_graph_id
                            MATCH(eddl:EditableDODiseaseList) WHERE eddl.id=editable 
                            SET eddl.list = Disease_graph_id '''
    send_to_neo4j(driver, read_editable_do_disease_list_elements)
    print("EditablDoDiseaseListElements", elapsed.total_seconds(), last_round.total_seconds())

    read_do_list_to_do_disease = ''' MATCH (eddl:EditableDODiseaseList) UNWIND eddl.list as do_diseases  MATCH (do:DODisease  {id:do_diseases})  
                    CREATE (eddl)-[:DODISEASE]->(do)  '''
    send_to_neo4j(driver, read_do_list_to_do_disease)
    print("Match editable do disease list", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableJAXDiseaseList(id)')
    read_editable_jax_disease_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableJaxDiseaseList.csv' AS row
                         WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.EditableDiseaseList_graph_id as id
                         MATCH(u:User) WHERE u.id=editor
                         CREATE (ejdl:EditableJAXDiseaseList :EditableObject {field:field, editDate:editDate,  id:id}) 
                         CREATE (ejdl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_jax_disease_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableJAXDiseaseList", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_jax_disease_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableJaxDiseaseListElements.csv' AS row
                            WITH row.EditableDiseaseList_graph_id as editable, split(row.Disease_graph_id, "|") AS Disease_graph_id
                            MATCH(ejdl:EditableJAXDiseaseList) WHERE ejdl.id=editable 
                            SET ejdl.list = Disease_graph_id '''
    send_to_neo4j(driver, read_editable_jax_disease_list_elements)
    print("EditablJaxDiseaseListElements", elapsed.total_seconds(), last_round.total_seconds())

    read_jax_list_to_jax_disease = ''' MATCH (ejdl:EditableJAXDiseaseList) UNWIND ejdl.list as jax_diseases  MATCH (jax:JaxDisease  {id:jax_diseases})  
                    CREATE (ejdl)-[:JAXDISEASE]->(jax)  '''
    send_to_neo4j(driver, read_jax_list_to_jax_disease)
    print("Match editable jax disease", elapsed.total_seconds(), last_round.total_seconds())

    send_to_neo4j(driver, 'CREATE INDEX ON :EditableOncoTreeDiseaseList(id)')
    read_editable_onco_disease_list = '''LOAD CSV WITH HEADERS FROM 'file:///EditableOncoTreeDiseaseList.csv' AS row
                         WITH row.field as field, row.edit_date as editDate, row.editor_id as editor, row.EditableDiseaseList_graph_id as id
                         MATCH(u:User) WHERE u.id=editor
                         CREATE (eotdl:EditableOncoTreeDiseaseList :EditableObject {field:field, editDate:editDate,  id:id}) 
                         CREATE (eotdl)-[:EDITED_BY]->(u)'''
    send_to_neo4j(driver, read_editable_onco_disease_list)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("EditableOncotreeDiseaseList", elapsed.total_seconds(), last_round.total_seconds())

    read_editable_oncotree_disease_list_elements = '''LOAD CSV WITH HEADERS FROM 'file:///EditableOncoTreeDiseaseListElements.csv' AS row
                            WITH row.EditableDiseaseList_graph_id as editable, split(row.Disease_graph_id, "|") AS Disease_graph_id
                            MATCH(eotdl:EditableOncoTreeDiseaseList) WHERE eotdl.id=editable 
                            SET eotdl.list = Disease_graph_id '''
    send_to_neo4j(driver, read_editable_oncotree_disease_list_elements)
    print("EditablOncotreeDiseaseListElements", elapsed.total_seconds(), last_round.total_seconds())

    read_oncotree_list_to_oncotree_disease = ''' MATCH (eotdl:EditableOncoTreeDiseaseList) UNWIND eotdl.list as ot_diseases  MATCH (ot:OncoTreeDisease  {id:ot_diseases})  
                    CREATE (eotdl)-[:ONCOTREEDISEASE]->(ot)  '''
    send_to_neo4j(driver, read_oncotree_list_to_oncotree_disease)
    print("Match editable onco trees", elapsed.total_seconds(), last_round.total_seconds())

    print('Stop 15 seconds')
    time.sleep(15)
    print('Start')

    send_to_neo4j(driver, 'CREATE INDEX ON :OntologicalDisease(id)')
    read_ontological_diseases = '''LOAD CSV WITH HEADERS FROM 'file:///ontological_diseases.csv' AS row
                                  WITH row.name as name, row.description as description, row.jaxDiseases  as jaxDiseases, row.doDiseases as doDiseases,  
                                  row.goDiseases as goDiseases, row.oncoTreeDiseases as oncoTreeDiseases, row.xrefs as xrefs, row. omniMaps as omniMaps,  
                                  row.synonyms as synonyms, row.graph_id as id
                                  MATCH(esn:EditableStatement) WHERE esn.id=name
                                  MATCH(esd:EditableStatement) WHERE esd.id=description
                                  MATCH(jd:EditableJAXDiseaseList) WHERE jd.id=jaxDiseases
                                  MATCH(dd:EditableDODiseaseList) WHERE dd.id=doDiseases
                                  MATCH(gd:EditableGODiseaseList) WHERE gd.id=goDiseases
                                  MATCH(od:EditableOncoTreeDiseaseList) WHERE od.id=oncoTreeDiseases
                                  MATCH(xreflist:EditableXRefList) WHERE xreflist.id=xrefs
                                  MATCH(omml:EditableOmniMapList) WHERE omml.id=omniMaps 
                                  MATCH(esyn:EditableStringList) WHERE esyn.id=synonyms
                                  CREATE (onto:OntologicalDisease { id:id})
                                  CREATE(onto) - [:NAMED]->(esn)
                                  CREATE(onto) - [:DESCRIBED_BY]->(esd)
                                  CREATE(onto) - [:JAXDISEASE]->(jd) 
                                  CREATE(onto) - [:DODISEASE]->(dd) 
                                  CREATE(onto) - [:GODISEASE]->(gd) 
                                  CREATE(onto) - [:ONCOTREEDISEASE]->(od) 
                                  CREATE(onto) - [:XREF]->(xreflist)
                                  CREATE(onto) - [:OMNIMAP]->(omml) 
                                  CREATE(onto) - [:ALSO_NAMED]->(esyn)'''
    send_to_neo4j(driver, read_ontological_diseases)
    elapsed, last_round, now = get_elapsed_time(now, start)
    print("OntologicalDisease", elapsed.total_seconds(), last_round.total_seconds())

    read_ontological_disease_parents = '''LOAD CSV WITH HEADERS FROM 'file:///ontological_parents.csv' AS row
                             WITH row.graph_id as id, split(row.parent, "|") AS parents
                             MATCH(onto:OntologicalDisease) WHERE onto.id=id 
                             SET onto.parents = parents'''
    send_to_neo4j(driver, read_ontological_disease_parents)
    print("onto  parents", elapsed.total_seconds(), last_round.total_seconds())

    read_ontological_disease_children = '''LOAD CSV WITH HEADERS FROM 'file:///ontological_children.csv' AS row
                                WITH row.graph_id as id, split(row.child, "|") AS children
                                MATCH(onto:OntologicalDisease) WHERE onto.id=id
                                SET onto.children = children'''
    send_to_neo4j(driver, read_ontological_disease_children)
    print("onto children", elapsed.total_seconds(), last_round.total_seconds())

    connect_onto_parents = ''' MATCH (onto:OntologicalDisease) UNWIND onto.parents as parent  MATCH (ot:OntologicalDisease  {id:parent})  
                                CREATE (onto)-[:PARENT]->(ot) '''
    send_to_neo4j(driver, connect_onto_parents)
    print("connect_onto_parents", elapsed.total_seconds(), last_round.total_seconds())

    connect_onto_children = ''' MATCH (onto:OntologicalDisease) UNWIND onto.children as child  MATCH (ot:OntologicalDisease  {id:child})  
                                   CREATE (onto)-[:CHILD]->(ot) '''
    send_to_neo4j(driver, connect_onto_children)
    print("connect_onto_children", elapsed.total_seconds(), last_round.total_seconds())

    driver.close()


if __name__ == "__main__":
    main()
