//! Promises the public pages must not make.
//!
//! A reader quotes a page word for word: "never", "with any broker", "the bot cannot get around it". Such a phrase
//! is either false under some condition or impossible to check, so the pages say what the code does and when. The
//! phrases are listed in `tests/docs_claims.txt` (the site check of the home page reads the same list). Checked: the
//! book (`docs/src`, the changelog aside: it is history), the READMEs and the agent index `docs/llms.py`. Skipped
//! when the book is not there (the crate as published has no `docs/`).

use std::path::{Path, PathBuf};

fn pages(dir: &Path, out: &mut Vec<PathBuf>) {
    for entry in std::fs::read_dir(dir).unwrap() {
        let path = entry.unwrap().path();
        if path.is_dir() {
            pages(&path, out);
        } else if path.extension().is_some_and(|e| e == "md")
            && path.file_name().is_some_and(|n| n != "changelog.md")
        {
            out.push(path);
        }
    }
}

/// The phrases of the list, lower case.
fn phrases(root: &Path) -> Vec<String> {
    std::fs::read_to_string(root.join("tests/docs_claims.txt"))
        .unwrap()
        .lines()
        .map(str::trim)
        .filter(|l| !l.is_empty() && !l.starts_with('#'))
        .map(str::to_lowercase)
        .collect()
}

/// `file:line: «phrase»` for each phrase of the list found in the text.
fn found(path: &Path, text: &str, phrases: &[String]) -> Vec<String> {
    let mut hits = Vec::new();
    for (n, line) in text.lines().enumerate() {
        let line = line.to_lowercase().replace('ё', "е");
        for phrase in phrases {
            if line.contains(phrase.as_str()) {
                hits.push(format!("{}:{}: «{phrase}»", path.display(), n + 1));
            }
        }
    }
    hits
}

#[test]
fn public_pages_make_no_unconditional_promises() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let book = root.join("docs/src");
    if !book.is_dir() {
        return;
    }
    let phrases = phrases(root);
    assert!(!phrases.is_empty(), "tests/docs_claims.txt has no phrases");
    let mut files = Vec::new();
    pages(&book, &mut files);
    files.extend(
        ["README.md", "crates/python/README.md", "docs/llms.py"]
            .iter()
            .map(|f| root.join(f)),
    );
    let hits: Vec<String> = files
        .iter()
        .flat_map(|f| found(f, &std::fs::read_to_string(f).unwrap(), &phrases))
        .collect();
    assert!(
        hits.is_empty(),
        "a page promises without a condition (tests/docs_claims.txt): say what the code does and when\n{}",
        hits.join("\n")
    );
}

#[test]
fn a_phrase_is_found_in_any_case_and_with_yo() {
    let phrases = vec!["не обойдет потолк".to_string()];
    assert_eq!(
        found(Path::new("p.md"), "Бот НЕ ОБОЙДЁТ ПОТОЛКИ.", &phrases),
        ["p.md:1: «не обойдет потолк»"]
    );
    assert!(
        found(
            Path::new("p.md"),
            "Потолки проверяются до отправки.",
            &phrases
        )
        .is_empty()
    );
}
