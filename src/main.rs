use anyhow::Result;

fn main() -> Result<()> {
    println!("loom-rt v{} — pre-runtime skeleton (Step 2)", env!("CARGO_PKG_VERSION"));
    Ok(())
}
