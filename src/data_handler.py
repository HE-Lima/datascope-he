# src/data_handler.py

#----------------------------------------------------------------------
#Libraries
#----------------------------------------------------------------------


#----------------------------------------------------------------------
def create_dataset_environment(dataset_name: str) -> dict:
    base_path = Path("./datasets/processed") / dataset_name
    subdirs = {
        "stages": base_path / "stages",
        "process": base_path / "process",
        "garbage": base_path / "garbage",
        "chunks": base_path / "chunks"
    }

    for path in subdirs.values():
        path.mkdir(parents=True, exist_ok=True)

    return {
        "project": base_path,
        **subdirs
    }












