use std::fs;
use std::io::Result;

fn main() -> Result<()> {
    let mut proto_files = vec![];

    for entry in fs::read_dir("src/raw-proto/").unwrap() {
        let entry_path = entry.unwrap().path();
        if entry_path.is_file() {
            if let Some(extension) = entry_path.extension() {
                if extension == "proto" {
                    proto_files.push(entry_path.display().to_string())
                }
            }
        }
    }

    let mut config = prost_build::Config::default();

    config.compile_protos(&proto_files, &["./src/raw-proto/"])?;
    Ok(())
}