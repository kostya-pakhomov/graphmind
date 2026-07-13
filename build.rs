use std::io::Result;

fn main() -> Result<()> {
    tonic_build::compile_protos("proto/memory.proto")?;
    println!("cargo:rerun-if-changed=proto/memory.proto");
    Ok(())
}
