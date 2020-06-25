import create_jax_disease_db
import create_do_disease_db
import create_oncotree_disease_db
import create_ontological_disease_db
import create_go_disease_db
import create_mcode_db
import create_omnidisease_db
import create_tcode_db

load_directory = 'C:/Users/irina.kurtz/PycharmProjects/Manuel/writeDiseases/load_files/'

def main():
    create_do_disease_db.main(load_directory)
    create_go_disease_db.main(load_directory)
    create_jax_disease_db.main(load_directory)
    create_omnidisease_db.main(load_directory)
    create_oncotree_disease_db.main(load_directory)
    create_tcode_db.main(load_directory)
    create_mcode_db.main(load_directory)
    create_ontological_disease_db.main(load_directory)

if __name__ == "__main__":
    main()