async function loadSoccer() {
  const container = document.getElementById("soccer-container");
  container.innerHTML = "Loading soccer cards...";

  try {
    const res = await fetch("data/sports/soccer.json");
    const data = await res.json();

    document.getElementById("last-updated").innerText =
      "Updated: " + new Date(data.updated_at).toLocaleString();

    container.innerHTML = "";

    data.tournaments.forEach(tournament => {
      const section = document.createElement("div");
      section.className = "tournament-section";

      section.innerHTML = `
        <h3>${tournament.name}</h3>
        <div class="cards"></div>
      `;

      const cardsDiv = section.querySelector(".cards");

      tournament.matches.forEach(match => {
        const card = document.createElement("div");
        card.className = "arb-card";

        card.innerHTML = `
          <div class="arb-rank">#${match.rank}</div>
          <div class="arb-match">${match.match}</div>
          <div class="arb-score">ArbScore: ${match.arb_score}</div>
          <div class="arb-edge">${match.best_edge}</div>
          <div class="arb-confidence">${match.confidence}</div>
          ${
            match.premium
              ? `<div class="arb-locked">🔒 ArbGraph & ArbStats (Premium)</div>`
              : `<div class="arb-free">Free insight</div>`
          }
        `;

        cardsDiv.appendChild(card);
      });

      container.appendChild(section);
    });
  } catch (e) {
    container.innerHTML = "Failed to load soccer data.";
  }
}

document.addEventListener("DOMContentLoaded", loadSoccer);
