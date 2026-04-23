"""GraphQL query and mutation definitions for stash_renamer."""

INTROSPECTION_TYPE_QUERY = """
query IntrospectType($typeName: String!) {
  __type(name: $typeName) {
    name
    fields(includeDeprecated: true) {
      name
      type {
        kind
        name
        ofType {
          kind
          name
          ofType {
            kind
            name
            ofType {
              kind
              name
              ofType {
                kind
                name
                ofType {
                  kind
                  name
                  ofType {
                    kind
                    name
                    ofType {
                      kind
                      name
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }
}
"""

MOVE_FILES_MUTATION = """
mutation moveFiles($input: MoveFilesInput!) {
  moveFiles(input: $input)
}
"""

FIND_SCENE_FILES_BY_ID_QUERY = """
query findScene($id: ID!) {
  findScene(id: $id) {
    id
    files {
      id
      path
    }
  }
}
"""

FIND_TAGS_QUERY = """
query findTags($filter: FindFilterType!, $tag_filter: TagFilterType!) {
  findTags(filter: $filter, tag_filter: $tag_filter) {
    tags { id name }
  }
}
"""
