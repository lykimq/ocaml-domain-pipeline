.PHONY: all build clean format fmt-check run check help

# Opam environment setup (evaluated once and reused)
OPAM_ENV = eval $$(opam env)

# Find all OCaml source files (excluding build directories)
FIND_OCAML = find . -type f \( -name "*.ml" -o -name "*.mli" \) \
		! -path "*/_build/*" \
		! -path "*/_opam/*" \
		! -path "*/.git/*"

# Default target
all: build

# Build the project
build:
	@echo "Building project..."
	@$(OPAM_ENV) && dune build

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	@$(OPAM_ENV) && dune clean

# Format all OCaml source files
format:
	@echo "Formatting all OCaml source files..."
	@$(OPAM_ENV) && $(FIND_OCAML) -exec ocamlformat --inplace {} \;
	@echo "Formatting complete"

# Check if code is properly formatted (read-only)
fmt-check:
	@echo "Checking code formatting..."
	@$(OPAM_ENV) && $(FIND_OCAML) \
		-exec sh -c 'ocamlformat --check {} || (echo "File {} is not formatted"; exit 1)' \;
	@echo "All files are properly formatted"

# Run the example pipeline
run: build
	@echo "Running example pipeline..."
	@$(OPAM_ENV) && dune exec examples/simple_pipeline.exe

# Run checks (format check + build)
check: fmt-check build
	@echo "All checks passed!"

# Show help
help:
	@echo "Available targets:"
	@echo "  make           - Build the project (default)"
	@echo "  make build     - Build the project"
	@echo "  make clean     - Clean build artifacts"
	@echo "  make format    - Format all OCaml source files"
	@echo "  make fmt-check - Check if code is properly formatted"
	@echo "  make run       - Run the example pipeline"
	@echo "  make check     - Check formatting and build"
	@echo "  make help      - Show this help message"

