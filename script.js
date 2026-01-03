async function loadArbs(league, containerId) {
  const res = await fetch(`data/${league}.json`);
  const data = await res.json();

  const container = document.getElementById(containerId);

  data.forEach(card => {
    const div = document.createElement("div");
    div.className = "card";

    div.innerHTML = `
      <div class="rank">Rank #${card.rank}</div>
      <div class="score">${card.match}</div>
      <p>Arb Score: <strong>${card.arb_score}</strong></p>
      <p class="${card.confidence.toLowerCase()}">
        Confidence: ${card.confidence}
      </p>
      <p>Best Edge: ${card.best_edge}</p>
    `;

    container.appendChild(div);
  });

  document.getElementById("lastUpdated").innerText =
    new Date().toUTCString();
}

loadArbs("afcon", "afcon-cards");
loadArbs("epl", "epl-cards");
