use clap::Parser;

#[derive(Parser)]
#[command(version, name = "uncleanliness")]
pub struct Cli {
    /// Reset the database.
    #[arg(short = 'r', long)]
    reset: bool,
}

impl Cli {
    /// Return true is restart flag is set.
    pub fn is_reset(&self) -> bool {
        self.reset
    }
}
