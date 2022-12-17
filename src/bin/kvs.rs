use clap::{Args, Parser, Subcommand};

#[derive(Parser, Debug)]
#[command(name = "kvs", author, version, about, long_about = None)]
struct Options {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand, Debug)]
enum Commands {
    /// Set the value of a string key to a string
    Set(Set),
    /// Get the string value of a given string key
    Get(Get),
    /// Remove a given key
    Rm(Remove),
}

#[derive(Args, Debug)]
struct Set {
    key: String,
    value: String,
}

#[derive(Args, Debug)]
struct Get {
    key: String,
}

#[derive(Args, Debug)]
struct Remove {
    key: String,
}

fn main() {
    let options = Options::parse();
    println!("{:?}", options);

    match &options.command {
        Commands::Set(_set) => {
            eprintln!("set unimplemented");
            std::process::exit(1);
        }
        Commands::Get(_get) => {
            eprintln!("get unimplemented");
            std::process::exit(1);
        }
        Commands::Rm(_rm) => {
            eprintln!("remove unimplemented");
            std::process::exit(1);
        }
    }
}
