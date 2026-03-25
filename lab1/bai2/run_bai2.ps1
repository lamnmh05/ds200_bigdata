$ErrorActionPreference = "Stop"

# ========= CONFIG =========
$PROJECT_DIR = "D:\Git_repo\DS200_BIGDATA\lab1\bai2"
$SRC_FILE    = Join-Path $PROJECT_DIR "GenreRatingAnalysis.java"
$CLASS_DIR   = Join-Path $PROJECT_DIR "build_classes"
$JAR_FILE    = Join-Path $PROJECT_DIR "GenreRatingAnalysis.jar"
$DATA_DIR    = "D:\Git_repo\DS200_BIGDATA\lab1\data"

$MOVIES_FILE   = Join-Path $DATA_DIR "movies.txt"
$RATINGS1_FILE = Join-Path $DATA_DIR "ratings_1.txt"
$RATINGS2_FILE = Join-Path $DATA_DIR "ratings_2.txt"

# Local mode output directory
$LOCAL_INPUT_DIR  = Join-Path $PROJECT_DIR "local_input"
$LOCAL_OUTPUT_DIR = Join-Path $PROJECT_DIR "local_output"
$LOCAL_MOVIES_FILE = Join-Path $LOCAL_INPUT_DIR "movies.txt"


# ========= CHECK =========
Write-Host "== Checking Java version =="
java -version

Write-Host "== Checking Hadoop version =="
hadoop version

if (!(Test-Path $SRC_FILE)) {
    throw "Khong tim thay file source: $SRC_FILE"
}
if (!(Test-Path $MOVIES_FILE)) {
    throw "Khong tim thay file: $MOVIES_FILE"
}
if (!(Test-Path $RATINGS1_FILE)) {
    throw "Khong tim thay file: $RATINGS1_FILE"
}
if (!(Test-Path $RATINGS2_FILE)) {
    throw "Khong tim thay file: $RATINGS2_FILE"
}

# ========= CLEAN BUILD =========
Write-Host "== Cleaning old build =="
if (Test-Path $CLASS_DIR) {
    Remove-Item -Recurse -Force $CLASS_DIR
}
if (Test-Path $JAR_FILE) {
    Remove-Item -Force $JAR_FILE
}

New-Item -ItemType Directory -Path $CLASS_DIR | Out-Null

# ========= COMPILE =========
Write-Host "== Compiling Java source =="
$cp = hadoop classpath
javac -encoding UTF-8 -classpath $cp -d $CLASS_DIR $SRC_FILE

# ========= CREATE JAR =========
Write-Host "== Creating jar =="
Push-Location $PROJECT_DIR
jar -cvf "GenreRatingAnalysis.jar" -C "build_classes" .
Pop-Location

# ========= LOCAL MODE PREP =========
Write-Host "== Preparing local input directories =="
if (Test-Path $LOCAL_INPUT_DIR) {
    Remove-Item -Recurse -Force $LOCAL_INPUT_DIR
}
if (Test-Path $LOCAL_OUTPUT_DIR) {
    Remove-Item -Recurse -Force $LOCAL_OUTPUT_DIR
}

New-Item -ItemType Directory -Path $LOCAL_INPUT_DIR | Out-Null

Write-Host "== Copying data files to local input =="
Copy-Item $RATINGS1_FILE $LOCAL_INPUT_DIR/
Copy-Item $RATINGS2_FILE $LOCAL_INPUT_DIR/
Copy-Item $MOVIES_FILE $LOCAL_MOVIES_FILE

Write-Host "== Listing local input =="
Get-ChildItem $LOCAL_INPUT_DIR

# ========= RUN JOB (LOCAL MODE) =========
Write-Host "== Running Hadoop job (LOCAL MODE) =="
hadoop jar $JAR_FILE GenreRatingAnalysis $LOCAL_INPUT_DIR $LOCAL_OUTPUT_DIR $LOCAL_MOVIES_FILE

# ========= SHOW RESULT =========
Write-Host "== Job output =="
if (Test-Path "$LOCAL_OUTPUT_DIR/part-r-00000") {
    Get-Content "$LOCAL_OUTPUT_DIR/part-r-00000"
} else {
    Write-Host "Output file not found at: $LOCAL_OUTPUT_DIR/part-r-00000"
    Write-Host "Output directory contents:"
    Get-ChildItem $LOCAL_OUTPUT_DIR -Recurse
}
