class EVTXHandler:
    def parse(self, file_path):
        print(f"[evtx] Stub parser for {file_path}")
        return [{"line_number": 1, "message": f"Parsed {file_path} as EVTX"}]
