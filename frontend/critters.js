// Critter appearance manifest. Real pixel art ("8BIT Pixel Cats" by 14
// Collective — see frontend/assets/pixel-cats/LICENSE-14collective.txt)
// replaces the earlier placeholder-shape approach.
//
// Only "yellow" is wired up for now: it's the only one of the 5
// purchased colors with a complete, cleanly-gridded animation set for
// all of idle/walking/running/sitting/loaf/licking/jumping (the other
// four are mostly a single static reference pose per action, plus
// Sitting and — except grey — Licking, which are clean everywhere).
// Adding another color later is just another entry here; the
// loading/animation code below is generic.
export const FRAME_SIZE = 64;

export const CRITTER_MANIFEST = {
  yellow: {
    idle: { frameCount: 12, frameRate: 6, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/idle.png" },
    walking: { frameCount: 8, frameRate: 10, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/walking.png" },
    running: { frameCount: 4, frameRate: 12, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/running.png" },
    sitting: { frameCount: 5, frameRate: 6, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/sitting.png" },
    loaf: { frameCount: 9, frameRate: 6, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/loaf.png" },
    licking: { frameCount: 15, frameRate: 10, repeat: -1, path: "assets/pixel-cats/yellow/unclothed/licking.png" },
    jumping: { frameCount: 7, frameRate: 12, repeat: 0, path: "assets/pixel-cats/yellow/unclothed/jumping.png" },
  },
};

export const DEFAULT_CRITTER = "yellow";

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
  const sprite = scene.add.sprite(x, y, textureKey(critter, "idle"));
  sprite.play(animKey(critter, "idle"));
  return sprite;
}
