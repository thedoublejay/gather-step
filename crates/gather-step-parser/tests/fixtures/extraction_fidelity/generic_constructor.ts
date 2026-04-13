import { Controller } from '@nestjs/common';

type Generic<T> = { value: T };
type Service<T> = { item: T };
type Repo<A, B> = { first: A; second: B };
type T = string;
type A = string;
type B = number;

@Controller('generic')
export class GenericController {
  constructor(
    private readonly service: Service<Generic<T>>,
    private readonly repo: Repo<A, B>,
  ) {}
}
