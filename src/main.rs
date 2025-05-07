use clap::Parser;
use colored::Colorize;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Error, ErrorKind, Read};
use std::path::{Path, PathBuf};

#[derive(Debug, Snafu)]
enum ProgramError {
    #[snafu(display("Could not find file {}.", path.display()))]
    FileNotFound { source: io::Error, path: PathBuf },
    #[snafu(display("Could not find log in {} with ending {}.", dir.display(), ending))]
    LogNotFound { dir: PathBuf, ending: String },
}

type PResult<T> = Result<T, ProgramError>;

#[derive(Parser)]
struct Cli {
    /// The ID of the job we want to find
    jobid: String,
}

fn get_subdirectories<P: AsRef<Path>>(start: P) -> PResult<Vec<PathBuf>> {
    Ok(fs::read_dir(&start)
        .context(FileNotFoundSnafu {
            path: start.as_ref().to_path_buf(),
        })?
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
fn build_job_map<P: AsRef<Path>>(start: P) -> PResult<HashMap<String, PathBuf>> {
    let mut jobmap = HashMap::new();
    let start = start.as_ref();

    for ymd in get_subdirectories(start)? {
        for hms in get_subdirectories(ymd)? {
            let submitit_dir = hms.join(".submitit");
            if !submitit_dir.exists() {
                continue;
            }
            for job in get_subdirectories(submitit_dir)? {
                if let Some(name) = job.file_name() {
                    jobmap.insert(name.to_str().unwrap().to_string(), job);
                }
            }
        }
    }
    Ok(jobmap)
}

fn get_log_content_or_error<P: AsRef<Path>>(dir: P, ending: &str) -> String {
    let log_fp = get_log(dir, ending);
    if log_fp.is_err() {
        return log_fp.err().unwrap().to_string();
    }
    let log_content = get_log_content(log_fp.unwrap());
    log_content.unwrap_or("Could not read log.".to_string())
}

fn get_log_content<P: AsRef<Path>>(filepath: P) -> Option<String> {
    let mut file = File::open(filepath).ok()?;
    let mut contents = String::new();
    file.read_to_string(&mut contents).ok()?;
    Some(contents)
}

fn get_log<P: AsRef<Path>>(dir: P, ending: &str) -> PResult<PathBuf> {
    let dir = dir.as_ref();
    for entry in fs::read_dir(dir).context(FileNotFoundSnafu {
        path: dir.to_path_buf(),
    })? {
        let f = entry.context(FileNotFoundSnafu {
            path: dir.to_path_buf(),
        })?;
        if f.file_type().unwrap().is_file()
            && f.path().extension().map_or(false, |ext| ext == ending)
        {
            return Ok(f.path());
        }
    }
    Err(LogNotFoundSnafu {
        dir: dir.to_path_buf(),
        ending: ending.to_string(),
    }
    .build())
}

fn main() {
    let cli = Cli::parse();
    let target = cli.jobid;
    let job_map = build_job_map("multirun").unwrap();
    let job_path = job_map[&target].clone();
    for ending in ["out", "err"] {
        let header = format!("Reporting {ending} file for job at {:?}:", job_path);
        let dashes = "-".repeat(header.len());

        println!(
            "{}\n{}\n{}\n",
            header.bold(),
            dashes.clone(),
            get_log_content_or_error(job_path.clone(), ending)
        );
    }
}
