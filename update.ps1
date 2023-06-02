# Set the path to the go.mod file
$filePath = ".\go.mod"

# Get the initial hash of the file
$initialHash = (Get-FileHash -Path $filePath).Hash

# Run go get to update dependencies
go get -u ./...

# Get the hash of the file after updating
$updatedHash = (Get-FileHash -Path $filePath).Hash

# If the hashes are the same, the file has not changed
if ($initialHash -eq $updatedHash) {
    Write-Output "No changes in go.mod file."
    exit
}

# If there are changes, add all files in the repo
git add .

# Get the latest git tag
$latestTag = git describe --tags $(git rev-list --tags --max-count=1)

# Split the tag into parts
$tagParts = $latestTag.Split(".")

# Increment the last part of the tag
$tagParts[2] = [int]$tagParts[2] + 1

# Join the parts back together to form the new tag
$newTag = $tagParts -join "."

# Commit the changes with a message
git commit -m "feat: updated deps"

# Create a new git tag with the new version
git tag $newTag

# Get current branch
$currentBranch = git rev-parse --abbrev-ref HEAD

# Push the changes and the new git tag to the current branch
git push origin $currentBranch
git push origin $newTag
