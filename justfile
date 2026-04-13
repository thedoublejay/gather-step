#!/usr/bin/env -S just --justfile

set windows-shell := ["powershell.exe", "-NoLogo", "-Command"]
set shell := ["bash", "-cu"]

_default:
    @just --list -u

alias r := ready
alias t := test
alias f := fix

init:
    cargo binstall -y bacon cargo-nextest cargo-deny cargo-shear cargo-insta just typos-cli

ready:
    typos
    cargo fmt --all --check
    cargo clippy --all-targets --all-features -- -D warnings
    cargo nextest run --all-features
    cargo deny check
    cargo shear

check:
    cargo check --all-targets --all-features --locked

test *args:
    cargo nextest run --all-features {{args}}

stress-mvcc:
    for run in $(seq 1 100); do cargo nextest run --profile stress --test mvcc_consistency; done

test-crate crate:
    cargo nextest run -p {{crate}} --all-features

lint:
    cargo clippy --all-targets --all-features -- -D warnings

fmt:
    cargo fmt --all

fix:
    cargo clippy --fix --allow-staged --all-targets --all-features
    just fmt
    typos -w

review:
    cargo insta review

build:
    cargo build --release -p gather-step

bench:
    cargo bench -p gather-step

bench-smoke:
    cargo bench -p gather-step --no-run

run *args:
    cargo run -p gather-step -- {{args}}

shear:
    cargo shear

deny:
    cargo deny check

typos:
    typos

outdated:
    cargo outdated -wR
