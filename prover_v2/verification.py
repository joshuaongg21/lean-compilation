import os
import sys
import json
import re
from tqdm import tqdm
import pandas as pd
from lean.verifier import Lean4ServerScheduler

def extract_lean_code(response, header=""):
    """Extract Lean code from the response data and combine with header."""
    if isinstance(response, list):
        response_text = response[-1] if response else ""
    else:
        response_text = str(response)
    
    extracted_code = ""
    marker = "### Complete Lean 4 Proof\n\n"

    if isinstance(response_text, str) and marker in response_text:
        _, after_marker = response_text.rsplit(marker, 1)
        if after_marker.lstrip().startswith("```lean4"):
            parts = after_marker.lstrip().split("```lean4", 1)
            if len(parts) > 1:
                extracted_code = parts[1].split("```")[0].strip()

    if not extracted_code and isinstance(response_text, str):
        if "```lean4" in response_text:
            parts = response_text.split("```lean4")
            if len(parts) > 1:
                extracted_code = parts[-1].split("```")[0].strip()
        elif "```lean" in response_text:
            parts = response_text.split("```lean")
            if len(parts) > 1:
                extracted_code = parts[-1].split("```")[0].strip()

    if not extracted_code:
        return ""

    proof_body_match = re.search(r":=\s*(.*)", extracted_code, re.DOTALL)
    if not proof_body_match:
        return header.strip()
    
    proof_body = proof_body_match.group(1).strip()

    sorry_pattern = re.compile(r":=\s*sorry", re.DOTALL)
    match = None
    num_match = 0
    for match in sorry_pattern.finditer(header):
        num_match += 1
    
    if match:
        start, end = match.span()
        new_code = header[:start] + f":= {proof_body}" + header[end:]
    else:
        new_code = header

    if num_match == 1:
        return new_code.strip()
    else:
        raise ValueError(f"No ':= sorry' pattern found in header to replace with proof body")


def format_error_check(output, status):
    """Format error information for easy tracing."""
    if status == 'NO_CODE_FOUND':
        return "NO_CODE_FOUND: No Lean code extracted from response"
    
    errors = output.get('errors', [])
    warnings = output.get('warnings', [])
    sorries = output.get('sorries', [])
    system_errors = output.get('system_errors', '')
    
    error_parts = []
    
    if system_errors:
        error_parts.append(f"SYSTEM_ERRORS: {system_errors}")
    
    if errors:
        error_details = [f"Error {i+1} (line {e.get('pos', {}).get('line', 'u')}, col {e.get('pos', {}).get('column', 'u')}): {e.get('data', str(e))}" for i, e in enumerate(errors)]
        error_parts.append("LEAN_ERRORS:\n" + "\n".join(error_details))
    
    if warnings:
        warning_details = [f"Warning {i+1} (line {w.get('pos', {}).get('line', 'u')}, col {w.get('pos', {}).get('column', 'u')}): {w.get('data', str(w))}" for i, w in enumerate(warnings)]
        error_parts.append("WARNINGS:\n" + "\n".join(warning_details))
    
    if sorries:
        error_parts.append(f"SORRIES: {len(sorries)} sorry statements found: {sorries}")
    
    if not error_parts:
        if status == 'COMPLETE': return "SUCCESS: Proof verified successfully"
        if status == 'PASS_WITH_ISSUES': return "PASS_WITH_ISSUES: Code compiles but may have minor issues"
        return "UNKNOWN: No specific errors found but verification failed"
    
    return "\n\n".join(error_parts)

def process_jsonl_file(file_path, max_entries=None, max_concurrent=8, timeout=300, lean_workspace='/dev/shm/mathlib4'):
    """Process JSONL file and verify Lean proofs."""
    if not os.path.exists(lean_workspace):
        print(f"Error: Lean workspace not found at {lean_workspace}")
        return []
    
    print(f"Using Lean workspace: {lean_workspace}")
    
    lean4_scheduler = Lean4ServerScheduler(
        max_concurrent_requests=max_concurrent, 
        timeout=timeout, 
        memory_limit=10, 
        name='proof_verifier'
    )
    
    results = []
    
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        
        lines = lines[:max_entries] if max_entries else lines
        print(f"Processing {len(lines)} entries...")
        
        request_ids = []
        entries = []
        
        for i, line in enumerate(tqdm(lines, desc="Submitting verification tasks")):
            try:
                entry = json.loads(line.strip())
            except json.JSONDecodeError:
                continue
            
            entries.append(entry)
            response = entry.get('response', '')
            formal_statement_header = entry.get('formal_statement', '')
            lean_code = extract_lean_code(response, header=formal_statement_header)
            
            if i < 3: # DEBUG PRINT
                print(f"\n--- DEBUG Entry {i} ---")
                print(f"Full code being sent to verifier:\n==================================================\n{lean_code}\n==================================================")

            if lean_code.strip():
                request_id = lean4_scheduler.submit_request(dict(
                    code=lean_code, ast=False, tactics=True, lean_workspace=lean_workspace
                ))
                request_ids.append((request_id, i, lean_code))
            else:
                if i < 3: 
                    print("Warning: No code extracted for this entry.")
                results.append({
                    'idx': entry.get('idx', i), 'formal_statement': formal_statement_header,
                    'response': response, 'verified_lean_code': '', 'verification_pass': False,
                    'verification_complete': False, 'verification_status': 'NO_CODE_FOUND',
                    'errors': 0, 'warnings': 0, 'sorries': 0, 'verify_time': 0,
                    'error_check': 'NO_CODE_FOUND: No Lean code extracted from response'
                })

        if request_ids:
            outputs = lean4_scheduler.get_all_request_outputs([rid for rid, _, _ in request_ids])
            
            for (rid, i, lean_code), output in zip(request_ids, outputs):
                entry = entries[i]

                if i < 3: 
                    print(f"\n--- DEBUG Verifier Output Entry {i} ---")
                    print(json.dumps(output, indent=2))

                if output.get('system_errors'): status = 'SYSTEM_ERROR'
                elif output.get('complete', False): status = 'COMPLETE'
                elif output.get('pass', False): status = 'PASS_WITH_ISSUES'
                else: status = 'FAIL'
                
                results.append({
                    'idx': entry.get('idx', i),
                    'formal_statement': entry.get('formal_statement', ''),
                    'response': entry.get('response', ''),
                    'verified_lean_code': lean_code,
                    'verification_pass': output.get('pass', False),
                    'verification_complete': output.get('complete', False),
                    'verification_status': status,
                    'errors': len(output.get('errors', [])),
                    'warnings': len(output.get('warnings', [])),
                    'sorries': len(output.get('sorries', [])),
                    'verify_time': output.get('verify_time', 0),
                    'error_check': format_error_check(output, status),
                    'full_verifier_output': output
                })
    finally:
        lean4_scheduler.close()
    
    df = pd.DataFrame(results)
    print("\nVerification Summary:")
    if not df.empty:
        print(f"Total entries processed: {len(df)}")
        print(f"Status counts: {df['verification_status'].value_counts().to_dict()}")
        print(f"Passed verification: {df['verification_pass'].sum()} ({df['verification_pass'].mean()*100:.2f}%)")
        print(f"Complete proofs: {df['verification_complete'].sum()} ({df['verification_complete'].mean()*100:.2f}%)")
        print(f"Average verification time: {df['verify_time'].mean():.2f}s")
    
    output_file = os.path.basename(file_path).replace('.jsonl', '_verification_results.jsonl')
    with open(output_file, 'w') as f:
        for result in results:
            json.dump(result, f, ensure_ascii=False)
            f.write('\n')
    print(f"Results saved to {output_file}")
    
    return results

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python verification.py <jsonl_file> [max_entries] [lean_workspace]")
        sys.exit(1)
    
    file_path = sys.argv[1]
    max_entries = int(sys.argv[2]) if len(sys.argv) > 2 else None
    lean_workspace = sys.argv[3] if len(sys.argv) > 3 else '/dev/shm/mathlib4'
    
    process_jsonl_file(file_path, max_entries, lean_workspace=lean_workspace)