use clap::Parser;
use std::collections::HashMap;
use std::fs;
use std::io::{self, Error, ErrorKind};
use std::path::{Path, PathBuf};

#[derive(Parser)]
struct Cli {
    /// The ID of the job we want to find
    jobid: String,
}

fn get_subdirectories<P: AsRef<Path>>(start: P) -> io::Result<Vec<PathBuf>> {
    Ok(fs::read_dir(start)?
        .into_iter()
        .filter_map(|entry| {
            let entry = entry.ok()?;
            let file_type = entry.file_type().ok()?;
            file_type.is_dir().then_some(entry.path())
        })
        .collect())
}

// Multirun dictionaries from submitit.slurm have the following structure:
// multirun/YYYY-MM-DD/hh-mm-ss/.submitit/<job-id>_<arr_id>
// We can do the following:
//   1. Flatten the nested datetime structs
//   2. Find all job ids and make a map: (job_id,path_to_job_id_dir)
//   3. Use job + arr id to find the correct job
fn build_job_map<P: AsRef<Path>>(start: P) -> io::Result<HashMap<String, PathBuf>> {
    let mut jobmap = HashMap::new();
    let start = start.as_ref();
    for ymd in get_subdirectories(start)? {
        for hms in get_subdirectories(ymd)? {
            for job in get_subdirectories(hms.join(".submitit"))? {
                if let Some(name) = job.file_name() {
                    jobmap.insert(name.to_str().unwrap().to_string(), job);
                }
            }
        }
    }
    Ok(jobmap)
}

fn get_log<P: AsRef<Path>>(dir: P, ending: &str) -> io::Result<PathBuf> {
    for entry in fs::read_dir(dir.as_ref())? {
        let f = entry?;
        if f.file_type()?.is_file() && f.path().extension().map_or(false, |ext| ext == ending) {
            return Ok(f.path());
        }
    }
    Err(Error::new(
        ErrorKind::Other,
        "Could not find log".to_string(),
    ))
}

fn main() {
    let cli = Cli::parse();
    let target = cli.jobid;
    let job_map = build_job_map("multirun").unwrap();
    println!("{:?}", job_map);
    println!("{:?}", get_log(&job_map[&target], "out").unwrap());
}
