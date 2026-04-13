type User = { id: string };
type Repository<T> = { entity: T };
type InnerRepo = { value: string };

export class OuterController {
  constructor(private readonly repo: Repository<User>) {}

  run() {
    class Nested {
      constructor(private readonly inner: InnerRepo) {}
    }

    return Nested;
  }
}
