// Critter appearance manifest. Real pixel art ("8BIT Pixel Cats" by 14
// Collective — see frontend/assets/pixel-cats/LICENSE-14collective.txt)
// replaces the earlier placeholder-shape approach.
//
// All 5 purchased colors have walking + sitting wired up. Each color's
// source sheets only ship idle/running/loaf/licking/jumping as a single
// static reference pose (or, for yellow, a cleanly-gridded set that
// isn't used here yet) — walking/sitting are the only actions with a
// full animation for every color.
//
// The "*-scaled.png" sheets are generated (not hand-drawn): the shipped
// source art draws each cat in only ~25-28px of a 64x64 tile, which
// renders small and low-detail in game. Regenerate them if the source
// files under a color's unclothed/ folder change — see
// frontend/assets/pixel-cats/generate-scaled-sheets.py.
export const FRAME_SIZE = 64;

export const CRITTER_MANIFEST = {
  yellow: {
    walking: { frameCount: 8, frameRate: 10, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/walking-scaled.png" },
    sitting: { frameCount: 5, frameRate: 6, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/sitting-scaled.png" },
  },
  siamese: {
    walking: { frameCount: 8, frameRate: 6, repeat: -1, path: "assets/pixel-cats/siamese/unclothed/walking-scaled.png" },
    sitting: { frameCount: 5, frameRate: 4, repeat: -1, path: "assets/pixel-cats/siamese/unclothed/sitting-scaled.png" },
  },
  pinkie: {
    walking: { frameCount: 8, frameRate: 6, repeat: -1, path: "assets/pixel-cats/pinkie/unclothed/walking-scaled.png" },
    sitting: { frameCount: 5, frameRate: 4, repeat: -1, path: "assets/pixel-cats/pinkie/unclothed/sitting-scaled.png" },
  },
  grey: {
    walking: { frameCount: 8, frameRate: 6, repeat: -1, path: "assets/pixel-cats/grey/unclothed/walking-scaled.png" },
    sitting: { frameCount: 5, frameRate: 4, repeat: -1, path: "assets/pixel-cats/grey/unclothed/sitting-scaled.png" },
  },
  black: {
    walking: { frameCount: 8, frameRate: 6, repeat: -1, path: "assets/pixel-cats/black/unclothed/walking-scaled.png" },
    sitting: { frameCount: 5, frameRate: 4, repeat: -1, path: "assets/pixel-cats/black/unclothed/sitting-scaled.png" },
  }
};

export const DEFAULT_CRITTER = "grey";

function textureKey(critter, action) {
  return `critter_${critter}_${action}`;
}

export function animKey(critter, action) {
  return `${critter}_${action}`;
}

export function preloadCritterTextures(scene) {
  for (const [critter, actions] of Object.entries(CRITTER_MANIFEST)) {
    for (const [action, def] of Object.entries(actions)) {
      scene.load.spritesheet(textureKey(critter, action), def.path, {
        frameWidth: FRAME_SIZE,
        frameHeight: FRAME_SIZE,
      });
    }
  }
}

export function registerCritterAnimations(scene) {
  for (const [critter, actions] of Object.entries(CRITTER_MANIFEST)) {
    for (const [action, def] of Object.entries(actions)) {
      const key = animKey(critter, action);
      if (scene.anims.exists(key)) continue;
      scene.anims.create({
        key,
        frames: scene.anims.generateFrameNumbers(textureKey(critter, action), { start: 0, end: def.frameCount - 1 }),
        frameRate: def.frameRate,
        repeat: def.repeat,
      });
    }
  }
}

// Creates a sprite for critter at (x, y), already playing its idle animation.
export function createCritterSprite(scene, critter, x, y) {
  const sprite = scene.add.sprite(x, y, textureKey(critter, "sitting"));
  sprite.play(animKey(critter, "sitting"));
  return sprite;
}
