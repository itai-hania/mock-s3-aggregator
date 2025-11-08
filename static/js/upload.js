const form = document.getElementById("upload-form");
const fileInput = document.getElementById("file");
const statusEl = document.getElementById("upload-status");

async function uploadFile(file) {
  const data = new FormData();
  data.append("file", file, file.name);

  const response = await fetch("/files", {
    method: "POST",
    body: data,
  });

  if (!response.ok) {
    const message = await readErrorMessage(response);
    throw new Error(message || `Upload failed with ${response.status}`);
  }

  const payload = await response.json();
  return payload.file_id;
}

async function readErrorMessage(response) {
  try {
    const body = await response.json();
    return body.detail;
  } catch {
    return null;
  }
}

function renderSummary(successful, failed) {
  if (!statusEl) {
    return;
  }

  statusEl.innerHTML = "";

  if (successful.length) {
    const heading = document.createElement("p");
    heading.textContent = `Uploaded ${successful.length} file(s):`;
    statusEl.appendChild(heading);

    const list = document.createElement("ul");
    successful.forEach(({ name, id }) => {
      const item = document.createElement("li");
      const link = document.createElement("a");
      link.href = `/ui/files/${id}`;
      link.textContent = `${name} → ${id}`;
      item.appendChild(link);
      list.appendChild(item);
    });
    statusEl.appendChild(list);
  }

  if (failed.length) {
    const heading = document.createElement("p");
    heading.textContent = `Failed ${failed.length} file(s):`;
    statusEl.appendChild(heading);

    const list = document.createElement("ul");
    failed.forEach(({ name, reason }) => {
      const item = document.createElement("li");
      item.textContent = `${name}: ${reason}`;
      list.appendChild(item);
    });
    statusEl.appendChild(list);
  }

  if (successful.length && failed.length === 0) {
    const note = document.createElement("p");
    note.textContent = "Refreshing to show new entries …";
    statusEl.appendChild(note);
    window.setTimeout(() => window.location.reload(), 1200);
  }
}

if (form && fileInput && statusEl) {
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    const files = Array.from(fileInput.files || []);
    if (!files.length) {
      return;
    }

    statusEl.textContent = `Uploading ${files.length} file(s)...`;

    const outcomes = await Promise.allSettled(files.map(uploadFile));
    const successful = [];
    const failed = [];

    outcomes.forEach((outcome, index) => {
      const file = files[index];
      if (outcome.status === "fulfilled") {
        successful.push({ name: file.name, id: outcome.value });
      } else {
        failed.push({
          name: file.name,
          reason: outcome.reason?.message || "Unknown error",
        });
      }
    });

    renderSummary(successful, failed);
  });
}
