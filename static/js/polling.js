const POLLABLE_STATUSES = new Set(["uploaded", "processing"]);

async function pollStatus(container) {
  const fileId = container.dataset.fileId;
  const statusEl = container.querySelector('[data-status]');
  const processedAtEl = container.querySelector('[data-processed-at]');
  const processingMsEl = container.querySelector('[data-processing-ms]');

  let active = true;

  async function tick() {
    if (!active) {
      return;
    }

    try {
      const response = await fetch(`/files/${fileId}`, {
        headers: {
          Accept: "application/json",
        },
      });

      if (!response.ok) {
        throw new Error(`Failed to fetch status: ${response.status}`);
      }

      const payload = await response.json();
      statusEl.textContent = capitalize(payload.status);

      if (payload.processed_at) {
        processedAtEl.textContent = new Date(payload.processed_at).toISOString();
      }

      if (payload.processing_ms) {
        processingMsEl.textContent = `${payload.processing_ms} ms`;
      }

      if (!POLLABLE_STATUSES.has(payload.status)) {
        active = false;
        window.location.reload();
        return;
      }
    } catch (error) {
      console.warn(error);
    }

    setTimeout(tick, 2000);
  }

  tick();
}

function capitalize(value) {
  return value.charAt(0).toUpperCase() + value.slice(1);
}

window.addEventListener("DOMContentLoaded", () => {
  const container = document.querySelector("[data-file-status]");
  if (!container) {
    return;
  }

  const shouldPoll = container.dataset.shouldPoll === "true";
  if (!shouldPoll) {
    return;
  }

  pollStatus(container);
});
