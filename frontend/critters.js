// Critter appearance manifest. No real art yet (see IMPLEMENTATION_PLAN.md's
// "no sprite/background art style decided" note) — each entry maps a
// critter type to a texture key and how to draw its placeholder shape.
// Swapping in real art later means replacing what draw() does, not
// touching scene.js's rendering/movement logic.
export const CRITTER_MANIFEST = {
  default: { textureKey: "critter_default", color: 0xe8a33d, radius: 14 },
};

export function registerCritterTextures(scene) {
  for (const critter of Object.values(CRITTER_MANIFEST)) {
    if (scene.textures.exists(critter.textureKey)) continue;
    const g = scene.make.graphics({ x: 0, y: 0 }, false);
    g.fillStyle(critter.color, 1);
    g.fillCircle(critter.radius, critter.radius, critter.radius);
    g.generateTexture(critter.textureKey, critter.radius * 2, critter.radius * 2);
    g.destroy();
  }
}
