import re

def read_disease_ontology(filepath):
    ontology_dict = {}
    children_dict = {}
    synonym_dict = {}
    ontology_names_dict = {}
    ontology_names_dict_by_doid = {}
    definitions_dict = {}

    is_a = []
    doid = ''
    name = None
    with open(filepath) as fp:
        for line in fp:
            if line.startswith('[Term]'):
                if len(is_a) > 0:
                    ontology_dict[doid] = is_a
                name = None
            elif line.startswith('id'):
                # id: DOID:0001816
                doid = line[4:].rstrip()
            elif line.startswith('name'):
                name = line[6:].rstrip().lower()
                if not name.startswith('obsolete'):
                    ontology_names_dict[name] =  doid
                    ontology_names_dict_by_doid[doid] = name
            elif line.startswith(('synonym')):
                synonym = re.findall(r'"([^"]*)"', line)[0].lower()
                if doid is not None and not name.startswith('obsolete'):
                    synonym_dict[synonym] = doid
            elif line.startswith('def'):
                definition = line[6:].rstrip().lower()
                definitions_dict[doid] = definition
            elif line.startswith('is_a'):
                # is_a: DOID:175 ! vascular cancer
                parent = line[6:line.find('!') - 1]
                is_a.append(parent)
                if parent in children_dict:
                    children = children_dict[parent]
                    children.append(doid)
                else:
                    children_dict[parent] = [doid]

    return synonym_dict, ontology_dict, ontology_names_dict, children_dict,ontology_names_dict_by_doid,definitions_dict




def find_all_children_names_inclusive(doid, children_dict, ontology_names_dict_by_doid, all_names):
    if all_names == None:
        all_names = []
        if doid in ontology_names_dict_by_doid:
            all_names.append(ontology_names_dict_by_doid[doid])
    if doid in children_dict:
        children = children_dict[doid]
        if children is not None:
            for child_doid in children:
                if child_doid in ontology_names_dict_by_doid:
                    all_names.append(ontology_names_dict_by_doid[child_doid])
                    # print(all_names)
                find_all_children_names_inclusive(child_doid, children_dict, ontology_names_dict_by_doid, all_names)
    return all_names


def main():
    synonym_dict, ontology_dict, ontology_names_dict, children_dict,ontology_names_dict_by_doid,definitions_dict = read_disease_ontology('data/doid.obo')
    doid = synonym_dict['skeletal system cancer']
    name = ontology_names_dict_by_doid[doid]
    definition = definitions_dict[doid]
    print(doid)
    print(name)
    print(definition)
    children = children_dict[doid]
    print(children)

    all_names = find_all_children_names_inclusive(doid, children_dict, ontology_names_dict_by_doid, None)
    print(all_names)


if __name__ == '__main__':
    main()