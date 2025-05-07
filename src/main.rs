use clap::{Parser, Subcommand};
use colored::Colorize;
use regex::Regex;
use snafu::{ResultExt, Snafu};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{self, Read};
use std::path::{Path, PathBuf};
use std::process::Command as ProcCommand;

#[derive(Debug, Snafu)]
enum ProgramError {
    #[snafu(display("Could not find file {}.", path.display()))]
    FileNotFound { source: io::Error, path: PathBuf },
    #[snafu(display("Could not find log in {} with ending {}.", dir.display(), ending))]
    LogNotFound { dir: PathBuf, ending: String },
}

type PResult<T> = Result<T, ProgramError>;

fn get_active_slurm_jobs() -> Vec<String> {
    let mut cmd = ProcCommand::new("squeue");
    cmd.arg("-h");
    cmd.arg("-o");
    cmd.arg("-%i");
    cmd.arg("--me");
    cmd.arg("-t");
    cmd.arg("RUNNING");
    let output = cmd.output().unwrap();
    if !output.status.success() {
        return Vec::new();
    }

    let stdout = String::from_utf8_lossy(&output.stdout);
    let job_ids = stdout
        .lines()
        .map(|line| line.trim().to_string())
        .filter(|line| !line.is_empty())
        .collect();

    job_ids
}

#[derive(Parser)]
struct Cli {
    #[clap(subcommand)]
    command: Command,
}

#[derive(Parser, Debug)]
struct ViewOpts {
    jobid: String,
}

#[derive(Parser, Debug)]
struct SearchOpts {
    pattern: String,
    #[arg(long, default_value_t = false)]
    ids: bool,
    #[arg(long, default_value_t = false)]
    active: bool,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// The ID of the job we want to find
    View(ViewOpts),
    Search(SearchOpts),
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

fn get_log_content_or_error_msg<P: AsRef<Path>>(dir: P, ending: &str) -> String {
    let log_fp = get_log_pathbuf(dir, ending);
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

fn get_log_pathbuf<P: AsRef<Path>>(dir: P, ending: &str) -> PResult<PathBuf> {
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

fn view(v: ViewOpts) {
    let target = v.jobid;
    let job_map = build_job_map("multirun").unwrap();
    let job_path = job_map[&target].clone();
    for ending in ["out", "err"] {
        let header = format!("Reporting {ending} file for job at {:?}:", job_path);
        let dashes = "-".repeat(header.len());

        println!(
            "{}\n{}\n{}\n",
            header.bold(),
            dashes.clone(),
            get_log_content_or_error_msg(job_path.clone(), ending)
        );
    }
}

fn search(s: SearchOpts) {
    let pattern = s.pattern;
    let regex = Regex::new(&pattern).unwrap();
    let job_map = build_job_map("multirun").unwrap();

    let mut entries: Vec<_> = job_map.iter().collect();
    entries.sort_by(|a, b| b.0.cmp(a.0));

    let active_jobs = if s.active {
        get_active_slurm_jobs()
    } else {
        Vec::new()
    };

    for (id, dir) in entries.iter() {
        if s.active && !active_jobs.contains(id) {
            continue;
        }
        let log_fp = get_log_pathbuf(dir, "out");
        if log_fp.is_err() {
            continue;
        }
        let log_content = get_log_content(log_fp.unwrap()).unwrap_or("".to_string());
        let matching_lines = log_content
            .lines()
            .filter_map(|line| {
                regex.is_match(line).then(|| {
                    regex.replace_all(line, |cap: &regex::Captures| cap[0].red().to_string())
                })
            })
            .collect::<Vec<_>>();

        if matching_lines.len() == 0 {
            continue;
        }

        if s.ids {
            println!("{id}");
        } else {
            let header = format!("{}:", dir.to_string_lossy());
            let dashes = "-".repeat(header.len());
            println!("{}\n{dashes}", header.bold());
            for line in matching_lines {
                println!("{}", line);
            }
        }
    }
}

fn main() {
    let cli = Cli::parse();
    match cli.command {
        Command::View(opts) => view(opts),
        Command::Search(opts) => search(opts),
    }
}
