import create_jax_disease_db
import create_do_disease_db
import create_oncotree_disease_db
import create_ontological_disease_db
import create_go_disease_db
import create_mcode_db
import create_omnidisease_db
import create_tcode_db
import create_id

load_directory = '../load_files/'
loader_id = 'user_20200422163431232329'
id_class = create_id.ID('', '', 0, 0, 0, 0, 0, 0)

def main(load_directory, loader_id, id_class):
    create_do_disease_db.main(load_directory, loader_id, id_class)
    create_go_disease_db.main(load_directory, loader_id, id_class)
    create_jax_disease_db.main(load_directory, loader_id, id_class)
    create_omnidisease_db.main(load_directory, loader_id, id_class)
    create_oncotree_disease_db.main(load_directory, loader_id, id_class)
    create_tcode_db.main(load_directory, loader_id, id_class)
    create_mcode_db.main(load_directory, loader_id, id_class)
    create_ontological_disease_db.main(load_directory, loader_id, id_class)
    print('All diseases are loaded')

if __name__ == "__main__":
    main(load_directory, loader_id, id_class)